"""Channel -> route mapping. One route = one Muse hook = one Muse side chat.

`routes.json`:

    {
      "default_route": "inbox",          # where unmatched channels go
      "alerts_route": "inbox",           # which route reports bridge_down/recovered
      "auto": {"mode": "auto", ...},     # optional: auto-create side chats (below)
      "routes": {
        "inbox":   {"side_chat": "Stoat · 收件箱"},
        "general": {"side_chat": "Stoat · #general", "match": ["01J...CHANNELID"],
                    "notes": "工程讨论频道，回复要技术化"},
        "dms":     {"side_chat": "Stoat · 私信", "match": ["type:DirectMessage"]}
      }
    }

(Path: STOAT_ROUTES_FILE, default <bridge dir>/routes.json.)

A selector in `match` is a channel id, `server:<server id>` or
`type:<channel_type>`. Resolution order: channel id, then server, then type,
then `default_route`, so every message lands on exactly one route and
nothing is dropped. Without a routes file there is a single route,
"default", which is the original one-hook setup.

Routes are resolved when a message is consumed (hook / worker), not when
the bridge stores it, so editing routes.json needs no bridge restart and
already-queued messages follow the new mapping.

Auto-created side chats (`"auto": {"mode": "auto"}`)
-----------------------------------------------------
When a channel that matches no route addresses the bot, and the channel is
eligible (type / server allowlists, total cap, hourly cap), its messages
resolve to AWAITING instead of the default route. The admin hook
(`stoatctl hook-poll --role admin`) then creates a route for it with
`status: "pending"` and wakes the admin worker, which registers the new
hook in Muse (hooks.add) with its own side chat. The new hook's first run
flips the route to `active`. A pending route that is not registered within
`pending_timeout_secs` becomes `failed`, and its channel falls back to the
default route. AWAITING messages older than the timeout also fall back, so
a missing admin hook can delay messages but never strand them.
"""
import fcntl
import json
import os
import re
import shlex
import time
from contextlib import contextmanager

from config import BASE

DEFAULT = "default"
AWAITING = "(awaiting side chat)"  # not a valid route name, so it never collides
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")
STATUSES = ("active", "pending", "failed")
AUTO_DEFAULTS = {
    "mode": "off",                         # off | auto
    "types": ["TextChannel", "Group"],     # add "DirectMessage" to give each DM its own side chat
    "servers": [],                         # server ids; empty = any server
    "max_routes": 20,                      # live auto-created routes at most
    "max_per_hour": 5,
    "pending_timeout_secs": 600,           # registration deadline for a new hook
    "side_chat_prefix": "Stoat · ",
    "admin_side_chat": "Stoat · 管理",
}


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
        self.routes = {k: dict(v) for k, v in (data.get("routes") or {}).items()}
        self.default_route = data.get("default_route") or DEFAULT
        self.alerts_route = data.get("alerts_route") or self.default_route
        self.auto = {**AUTO_DEFAULTS, **(data.get("auto") or {})}
        if self.auto["mode"] not in ("off", "auto"):
            raise RouteError(f"auto.mode must be off or auto, not {self.auto['mode']!r}")
        self.routes.setdefault(self.default_route, {"side_chat": "Stoat"})
        for name, route in self.routes.items():
            if not NAME_RE.match(name):
                raise RouteError(f"bad route name {name!r}: use [a-z0-9_-], max 48 chars")
            route.setdefault("side_chat", f"Stoat · {name}")
            route.setdefault("match", [])
            if route.get("status", "active") not in STATUSES:
                raise RouteError(f"route {name!r}: bad status {route['status']!r}")
        if self.routes[self.default_route].get("status", "active") != "active":
            raise RouteError("the default route must be active")
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

    # --- status -------------------------------------------------------------------

    def status(self, name):
        return self.routes[name].get("status", "active")

    def usable(self, name, now=None):
        """Active, or pending and still inside its registration window."""
        status = self.status(name)
        if status == "pending":
            now = time.time() if now is None else now
            return now - self.routes[name].get("pending_since", 0) \
                <= self.auto["pending_timeout_secs"]
        return status == "active"

    # --- resolution -----------------------------------------------------------

    def candidates(self, channel, channel_type=None, server=None):
        """Routes whose selectors match, most specific first (usable or not)."""
        found = [self._by_channel.get(channel),
                 server and self._by_server.get(server),
                 channel_type and self._by_type.get(channel_type)]
        return [c for c in found if c]

    def route_for(self, channel, channel_type=None, server=None, received_at=None, now=None):
        now = time.time() if now is None else now
        found = self.candidates(channel, channel_type, server)
        for name in found:
            if self.usable(name, now):
                return name
        if not found and self.auto_eligible(channel_type, server, now) and (
                received_at is None
                or now - received_at <= self.auto["pending_timeout_secs"]):
            return AWAITING
        return self.default_route

    def route_for_record(self, rec, now=None):
        return self.route_for(rec.get("channel"), rec.get("channel_type"), rec.get("server"),
                              rec.get("received_at"), now)

    def is_unrouted(self, channel, channel_type=None, server=None):
        """True when the channel only reaches the default route as a fallback."""
        return not self.candidates(channel, channel_type, server)

    # --- auto policy ----------------------------------------------------------------

    def auto_routes(self):
        return {n: r for n, r in self.routes.items() if r.get("auto")}

    def auto_eligible(self, channel_type, server, now=None):
        a = self.auto
        if a["mode"] != "auto" or channel_type not in a["types"]:
            return False
        if a["servers"] and server not in a["servers"]:
            return False
        now = time.time() if now is None else now
        live = [r for r in self.auto_routes().values() if r.get("status") != "failed"]
        if len(live) >= a["max_routes"]:
            return False
        recent = [r for r in self.auto_routes().values()
                  if now - r.get("created_at", 0) < 3600]
        return len(recent) < a["max_per_hour"]

    def provision(self, channel, name_hint, label, channel_type, now=None):
        """Create a pending auto route for one channel. Returns its name."""
        now = time.time() if now is None else now
        base = slug(name_hint)
        name, n = base, 2
        while name in self.routes:
            name, n = f"{base}-{n}", n + 1
        prefix = self.auto["side_chat_prefix"]
        side_chat = f"{prefix}{label}" if channel_type == "DirectMessage" \
            else f"{prefix}#{label}"
        self.routes[name] = {"side_chat": side_chat, "match": [channel], "auto": True,
                             "status": "pending", "created_at": now, "pending_since": now}
        self._index()
        return name

    def activate(self, name, now=None):
        route = self.routes[name]
        route["status"] = "active"
        route["activated_at"] = time.time() if now is None else now
        route.pop("pending_since", None)

    def fail(self, name, reason, now=None):
        route = self.routes[name]
        route["status"] = "failed"
        route["failed_at"] = time.time() if now is None else now
        route["failed_reason"] = reason
        route.pop("pending_since", None)

    def expired_pending(self, now=None):
        now = time.time() if now is None else now
        return [n for n in self.routes if self.status(n) == "pending" and not self.usable(n, now)]

    # --- listing --------------------------------------------------------------------

    def names(self):
        return sorted(self.routes, key=lambda n: (n != self.default_route, n))

    def info(self, name):
        route = self.routes[name]
        return {"route": name, "side_chat": route["side_chat"], "status": self.status(name),
                "notes": route.get("notes"), "default": name == self.default_route}

    # --- editing ------------------------------------------------------------------

    def add(self, name, selectors=(), side_chat=None, notes=None):
        if not NAME_RE.match(name):
            raise RouteError(f"bad route name {name!r}")
        for other in self.routes.values():  # a selector belongs to one route only
            other["match"] = [s for s in other["match"] if s not in selectors]
        route = self.routes.setdefault(name, {"side_chat": f"Stoat · {name}", "match": []})
        route["match"] = sorted(set(route["match"]) | set(selectors))
        if side_chat:
            route["side_chat"] = side_chat
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
        out = {"default_route": self.default_route, "alerts_route": self.alerts_route}
        if self.auto != AUTO_DEFAULTS:
            out["auto"] = self.auto
        out["routes"] = self.routes
        return out

    def save(self):
        tmp = f"{self.path}.tmp.{os.getpid()}"
        with open(tmp, "w") as f:
            json.dump(self.to_json(), f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, self.path)


@contextmanager
def locked_routes(cfg):
    """Read-modify-write of routes.json under a lock (admin hook, first-run
    activation and operator commands may all write it)."""
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
# Generated by stoatctl. {what}
# Muse side chat: {side_chat}
# Register this file as its own Muse hook; deliver its worker output to that side chat.
export STOAT_HOOK_ROLE={role}
export STOAT_ROUTE={route_q}
export STOAT_BRIDGE_DIR={base_q}
export STOAT_DATA_DIR={data_q}
export STOAT_ROUTES_FILE={routes_q}
source "$STOAT_BRIDGE_DIR/hook/stoat-watch.sh"
"""

ADMIN_HOOK_ID = "stoat-admin"


def _write_wrapper(cfg, routes, path, role, route, side_chat, what):
    with open(path, "w") as f:
        f.write(WRAPPER.format(what=what, side_chat=side_chat, role=role,
                               route_q=shlex.quote(route), base_q=shlex.quote(BASE),
                               data_q=shlex.quote(cfg.data_dir),
                               routes_q=shlex.quote(routes.path)))
    os.chmod(path, 0o755)


def hook_definitions(cfg, routes, poll=5, timeout=60, write=False):
    """One Muse hook per live route, plus the admin hook when auto mode is on.
    Failed routes are left out: they must not be registered."""
    out_dir = cfg.hooks_dir
    defs = []
    if write:
        os.makedirs(out_dir, exist_ok=True)
    if routes.auto["mode"] == "auto":
        script = os.path.join(out_dir, "_admin.sh")
        if write:
            _write_wrapper(cfg, routes, script, "admin", "", routes.auto["admin_side_chat"],
                           "Admin hook: provisions per-channel hooks.")
        defs.append({"id": ADMIN_HOOK_ID, "script_path": script,
                     "poll_interval_secs": poll, "script_timeout_secs": timeout,
                     "instructions_file": os.path.join(BASE, "hook", "ADMIN.md"),
                     "deliver_to": f"side_chat:{routes.auto['admin_side_chat']}",
                     "role": "admin"})
    for name in routes.names():
        if routes.status(name) == "failed":
            continue
        info = routes.info(name)
        script = os.path.join(out_dir, f"{name}.sh")
        if write:
            _write_wrapper(cfg, routes, script, "route", name, info["side_chat"],
                           f"Route: {name}")
        defs.append({"id": f"stoat-{name}", "script_path": script,
                     "poll_interval_secs": poll, "script_timeout_secs": timeout,
                     "instructions_file": os.path.join(BASE, "hook", "WORKER.md"),
                     "deliver_to": f"side_chat:{info['side_chat']}", "route": name,
                     "status": info["status"]})
    if write:
        tmp = os.path.join(out_dir, f"hooks.json.tmp.{os.getpid()}")
        with open(tmp, "w") as f:
            json.dump(defs, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, os.path.join(out_dir, "hooks.json"))
    return defs
