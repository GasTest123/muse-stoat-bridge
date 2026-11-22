"""Channel -> route mapping: the bridge's own multi-chat split.

One route = one queue, one worker lease, one wake state and one Muse hook, so
different Stoat channels are handled by separate worker turns and never see
each other's messages. Every hook delivers wherever Muse delivers hook output
(its main chat); the bridge does not create or address Muse side chats.

`routes.json`:

    {
      "default_route": "inbox",          # where unmatched channels go
      "alerts_route": "inbox",           # which route reports bridge_down/recovered
      "routes": {
        "inbox":   {},
        "general": {"match": ["01J...CHANNELID"], "notes": "工程讨论频道，回复要技术化"},
        "dms":     {"match": ["type:DirectMessage"]}
      }
    }

(Path: STOAT_ROUTES_FILE, default <bridge dir>/routes.json.)

A selector in `match` is a channel id, `server:<server id>` or
`type:<channel_type>`. Resolution order: channel id, then server, then type,
then `default_route`, so every message lands on exactly one route and
nothing is dropped. Without a routes file there is a single route,
"default", which is the one-hook setup.

Routes are resolved when a message is consumed (hook / worker), not when
the bridge stores it, so editing routes.json needs no bridge restart and
already-queued messages follow the new mapping.
"""
import fcntl
import json
import os
import re
import shlex
from contextlib import contextmanager

from config import BASE

DEFAULT = "default"
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
# Keys earlier versions wrote for Muse side chats / auto provisioning; dropped on load.
LEGACY_ROUTE_KEYS = ("side_chat", "auto", "status", "created_at", "pending_since",
                     "activated_at", "failed_at", "failed_reason")
class RouteError(Exception):
    pass


def slug(text):
    s = re.sub(r"[^a-z0-9_-]+", "-", (text or "").lower()).strip("-_")[:48]
    return s or "channel"


def load_directory(cfg):
    """Channels the bridge has seen: {id: {name, channel_type, server, server_name}}."""
    try:
        with open(cfg.path("channel_directory.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


class Routes:
    def __init__(self, data=None, path=None):
        data = data or {}
        self.path = path
        routes = data.get("routes") or {}
        # A route that an older version gave up on ("failed") has no hook: drop it
        # so its channels fall back to the default route, as they did then.
        self.routes = {k: {f: x for f, x in v.items() if f not in LEGACY_ROUTE_KEYS}
                       for k, v in routes.items() if v.get("status") != "failed"}
        self.default_route = data.get("default_route") or DEFAULT
        self.alerts_route = data.get("alerts_route") or self.default_route
        self.routes.setdefault(self.default_route, {})
        for name, route in self.routes.items():
            if not NAME_RE.match(name):
                raise RouteError(f"bad route name {name!r}: use [a-z0-9_-], max 48 chars")
            route.setdefault("match", [])
        if self.alerts_route not in self.routes:
            raise RouteError(f"alerts_route {self.alerts_route!r} is not a route")
        self._index()

    @classmethod
    def load(cls, cfg):
        path = cfg.routes_file
        try:
            with open(path) as f:
                return cls(json.load(f), path)
        except FileNotFoundError:
            return cls(None, path)
        except ValueError as e:
            raise RouteError(f"{path}: invalid JSON: {e}")

    def _index(self):
        self._by_channel, self._by_server, self._by_type = {}, {}, {}
        for name, route in self.routes.items():
            for sel in route["match"]:
                if sel.startswith("server:"):
                    table, key = self._by_server, sel[7:]
                elif sel.startswith("type:"):
                    table, key = self._by_type, sel[5:]
                else:
                    table, key = self._by_channel, sel
                if key in table and table[key] != name:
                    raise RouteError(f"selector {sel!r} is claimed by both "
                                     f"{table[key]!r} and {name!r}")
                table[key] = name

    # --- resolution -----------------------------------------------------------

    def candidates(self, channel, channel_type=None, server=None):
        """Routes whose selectors match, most specific first."""
        found = [self._by_channel.get(channel),
                 server and self._by_server.get(server),
                 channel_type and self._by_type.get(channel_type)]
        return [c for c in found if c]

    def route_for(self, channel, channel_type=None, server=None):
        found = self.candidates(channel, channel_type, server)
        return found[0] if found else self.default_route

    def route_for_record(self, rec):
        return self.route_for(rec.get("channel"), rec.get("channel_type"), rec.get("server"))

    def is_unrouted(self, channel, channel_type=None, server=None):
        """True when the channel only reaches the default route as a fallback."""
        return not self.candidates(channel, channel_type, server)

    # --- listing --------------------------------------------------------------------

    def names(self):
        return sorted(self.routes, key=lambda n: (n != self.default_route, n))

    def info(self, name):
        route = self.routes[name]
        return {"route": name, "notes": route.get("notes"),
                "default": name == self.default_route}

    # --- editing ------------------------------------------------------------------

    def add(self, name, selectors=(), notes=None):
        if not NAME_RE.match(name):
            raise RouteError(f"bad route name {name!r}")
        for other in self.routes.values():  # a selector belongs to one route only
            other["match"] = [s for s in other["match"] if s not in selectors]
        route = self.routes.setdefault(name, {"match": []})
        route["match"] = sorted(set(route["match"]) | set(selectors))
        if notes is not None:
            route["notes"] = notes
        self._index()
        return route

    def remove(self, name):
        if name == self.default_route:
            raise RouteError("the default route cannot be removed")
        if self.alerts_route == name:
            self.alerts_route = self.default_route
        self.routes.pop(name)
        self._index()

    def to_json(self):
        return {"default_route": self.default_route, "alerts_route": self.alerts_route,
                "routes": self.routes}

    def save(self):
        tmp = f"{self.path}.tmp.{os.getpid()}"
        with open(tmp, "w") as f:
            json.dump(self.to_json(), f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, self.path)


@contextmanager
def locked_routes(cfg):
    """Read-modify-write of routes.json under a lock (operator commands)."""
    os.makedirs(os.path.dirname(cfg.routes_file), exist_ok=True)
    with open(cfg.routes_file + ".lock", "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            routes = Routes.load(cfg)
            before = json.dumps(routes.to_json(), sort_keys=True)
            yield routes
            if json.dumps(routes.to_json(), sort_keys=True) != before:
                routes.save()
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def suggested_name(info, channel_id):
    if info.get("name"):
        return slug(info["name"])
    return slug(channel_id[-8:])


# --- Muse hook definitions -----------------------------------------------------------

WRAPPER = """#!/usr/bin/env bash
# Generated by stoatctl. Route: {route}
# Register this file as its own Muse hook (instructions: hook/WORKER.md).
export STOAT_ROUTE={route_q}
export STOAT_BRIDGE_DIR={base_q}
export STOAT_DATA_DIR={data_q}
export STOAT_ROUTES_FILE={routes_q}
source "$STOAT_BRIDGE_DIR/hook/stoat-watch.sh"
"""


def hook_definitions(cfg, routes, poll=5, timeout=60, write=False):
    """One Muse hook per route. With write=True also writes the wrapper scripts
    and hooks.json into cfg.hooks_dir."""
    out_dir = cfg.hooks_dir
    defs = []
    if write:
        os.makedirs(out_dir, exist_ok=True)
    for name in routes.names():
        script = os.path.join(out_dir, f"{name}.sh")
        if write:
            with open(script, "w") as f:
                f.write(WRAPPER.format(route=name, route_q=shlex.quote(name),
                                       base_q=shlex.quote(BASE),
                                       data_q=shlex.quote(cfg.data_dir),
                                       routes_q=shlex.quote(routes.path)))
            os.chmod(script, 0o755)
        defs.append({"id": f"stoat-{name}", "script_path": script,
                     "poll_interval_secs": poll, "script_timeout_secs": timeout,
                     "instructions_file": os.path.join(BASE, "hook", "WORKER.md"),
                     "route": name})
    if write:
        for stale in os.listdir(out_dir):   # e.g. the old _admin.sh, removed routes
            if stale.endswith(".sh") and stale[:-3] not in routes.routes:
                os.remove(os.path.join(out_dir, stale))
        tmp = os.path.join(out_dir, f"hooks.json.tmp.{os.getpid()}")
        with open(tmp, "w") as f:
            json.dump(defs, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, os.path.join(out_dir, "hooks.json"))
    return defs
