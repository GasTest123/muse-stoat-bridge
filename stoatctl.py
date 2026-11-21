#!/usr/bin/env python3
"""stoatctl: the command-line surface the Muse hooks and workers use.

Routing: every Stoat channel resolves to a route (src/routes.py), and every
route is one Muse hook delivering to one Muse side chat. `--route NAME`
(or STOAT_ROUTE) scopes a command to that route: the queue, worker lease,
wake state and hook lock are all per route, and a worker cannot touch
messages or channels of another route. Without --route a command sees
everything (single-hook mode, and the operator's view).

Worker commands (each also refreshes the route's worker lease, so its hook
knows someone is on duty and does not wake a second worker):
  peek [--count]                        pending messages, JSON lines
  history <channel> [--limit N] [--around MSG] [--chain MSG]
  send <msg_id> [--part k/n] [--reply-to MSG|none] [--mention UID]...
                                        durable reply; body on stdin
  no-reply <msg_id> --reason TEXT       record a deliberate non-reply
  typing <channel> start|stop
  lease [--release]
  warm-wait [--idle S] [--total S]      prints NEW_MESSAGES | IDLE_TIMEOUT | TOTAL_CAP
Hook command:
  hook-poll [--window S] [--role route|admin]
                                        TSV line: wake|silent <TAB> reason <TAB> payload;
                                        the admin role provisions per-channel side chats;
                                        with STOAT_AUTOSTART=1 it also starts a stopped bridge
Operator commands (humans, from the main chat or a terminal):
  status | sends | release <key> --confirm-not-sent | compact | whoami
  bridge start|stop|status              run bridge.py detached (log: <data>/bridge.log)
  routes list | discover | add NAME SELECTOR... | add-each | remove NAME | hooks [--write]
         auto [--mode auto|off ...] | activate NAME | fail NAME --reason R

Exit codes for send / no-reply are documented in src/delivery.py; 4 is also
returned when a message or channel belongs to another route.
"""
import argparse
import fcntl
import json
import os
import shlex
import signal
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "src"))  # library modules live in src/

import ulidutil  # noqa: E402
from config import Config  # noqa: E402
from delivery import FAIL_CLOSED, Delivery, bot_identity  # noqa: E402
from inbox_store import Inbox  # noqa: E402
from routes import (AWAITING, RouteError, Routes, hook_definitions,  # noqa: E402
                    load_directory, locked_routes, suggested_name)
from send_state import SendState, StateCorruptError  # noqa: E402
from stoat_api import StoatAPI  # noqa: E402

LEASE_BUSY_TTL = 120
LEASE_WARM_TTL = 45
# `lease --hold N`: a worker about to block on something slow (video
# generation) keeps its busy lease without heartbeats for up to this long.
LEASE_MAX_HOLD = 1800
WAKE_COOLDOWN = 240
PROVISION_RETRY_AFTER = 180  # re-announce a pending route the admin has not registered
PROVISION_ANNOUNCE_MAX = 2
MAX_WAKES_PER_MESSAGE = 3
BRIDGE_STALE = 60
BRIDGE_ALERT_AFTER = 120
ALERT_REPEAT = 3600


def emit(obj, stream=None):
    print(json.dumps(obj, ensure_ascii=False), file=stream or sys.stdout, flush=True)


def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(path, obj):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _mtime(path):
    try:
        return os.stat(path).st_mtime_ns
    except FileNotFoundError:
        return None


class WrongRoute(Exception):
    pass


class Ctx:
    def __init__(self, cfg, route=None):
        self.cfg = cfg
        self.inbox = Inbox(cfg.path("inbox.jsonl"))
        self.routes_mtime = _mtime(cfg.routes_file)
        self.routes = Routes.load(cfg)
        if route is not None and route not in self.routes.routes:
            raise RouteError(f"unknown route {route!r}; known: {', '.join(self.routes.names())}")
        self.route = route
        self._api = None

    def _file(self, stem, ext, route=None):
        route = self.route if route is None else route
        return self.cfg.path(f"{stem}.{route}.{ext}" if route else f"{stem}.{ext}")

    @property
    def lease_path(self):
        return self._file("worker_lease", "json")

    @property
    def api(self):
        if self._api is None:
            self._api = StoatAPI(self.cfg)
        return self._api

    def delivery(self):
        return Delivery(self.cfg, self.api, self.inbox, SendState(self.cfg.path("send_state.json")))

    # --- route scoping ------------------------------------------------------------

    def pending(self, route=None):
        route = self.route if route is None else route
        records = self.inbox.pending()
        if not route:
            return records
        return [r for r in records if self.routes.route_for_record(r) == route]

    def check_msg(self, msg_id):
        if not self.route:
            return
        rec = self.inbox.get(msg_id)
        if rec is None:
            return  # delivery reports unknown-source itself
        actual = self.routes.route_for_record(rec)
        if actual != self.route:
            raise WrongRoute(f"{msg_id} belongs to route {actual!r}, not {self.route!r}")

    def check_channel(self, channel):
        if not self.route:
            return
        info = load_directory(self.cfg).get(channel)
        if info is None:
            # Not flushed to the directory yet (brand-new channel): the queued
            # record carries the same type/server fields.
            info = next((r for r in reversed(self.inbox.pending()) if r["channel"] == channel),
                        {})
        actual = self.routes.route_for(channel, info.get("channel_type"), info.get("server"))
        if actual != self.route:
            raise WrongRoute(f"channel {channel} belongs to route {actual!r}, "
                             f"not {self.route!r}")

    # --- worker lease ---------------------------------------------------------------

    def lease(self, route=None):
        return read_json(self._file("worker_lease", "json", route))

    @staticmethod
    def lease_active(lease, now):
        if not lease:
            return False
        age = now - lease.get("heartbeat", 0)
        if lease.get("mode") == "warm":
            return age <= LEASE_WARM_TTL and lease.get("until", 0) > now
        return age <= LEASE_BUSY_TTL or lease.get("hold_until", 0) > now

    def touch_lease(self, mode="busy", until=None, offered=None, hold_until=None):
        now = time.time()
        cur = self.lease()
        active = self.lease_active(cur, now)
        since = cur.get("since", now) if active else now
        lease = {"mode": mode, "heartbeat": now, "since": since,
                 # msg_ids warm-wait already handed to this worker session
                 "offered": sorted(set(cur.get("offered", []) if active else [])
                                   | set(offered or []))}
        if until is not None:
            lease["until"] = until
        if mode == "busy":
            # An explicit hold survives the busy heartbeats issued meanwhile.
            hold = hold_until if hold_until is not None else \
                (cur.get("hold_until", 0) if active and cur.get("mode") == "busy" else 0)
            if hold > now:
                lease["hold_until"] = hold
        write_json(self.lease_path, lease)
        return lease

    def drop_lease(self):
        try:
            os.remove(self.lease_path)
        except FileNotFoundError:
            pass


def wrong_route(e):
    emit({"result": "wrong-route", "exit": FAIL_CLOSED, "detail": str(e)})
    return FAIL_CLOSED


# --- worker commands ------------------------------------------------------------------


def cmd_peek(ctx, args):
    ctx.touch_lease()
    pending = ctx.pending()
    if args.count:
        print(len(pending))
        return 0
    for rec in pending[: args.limit]:
        emit(rec)
    return 0


def _fmt_message(m, users, me):
    user = users.get(m.get("author"), {})
    return {
        "id": m["_id"],
        "time": time.strftime("%Y-%m-%d %H:%M:%S",
                              time.localtime(ulidutil.time_ms(m["_id"]) / 1000)),
        "author": m.get("author"),
        "author_name": user.get("display_name") or user.get("username"),
        "is_bot": bool(user.get("bot")),
        "is_me": m.get("author") == me,
        "content": m.get("content") or "",
        "replies": m.get("replies") or [],
        "mentions": m.get("mentions") or [],
        "attachments": [a.get("filename") for a in m.get("attachments") or []],
    }


def cmd_history(ctx, args):
    try:
        ctx.check_channel(args.channel)
    except WrongRoute as e:
        return wrong_route(e)
    ctx.touch_lease()
    try:
        me = bot_identity(ctx.cfg, ctx.api)
        if args.chain:
            chain, users = [], {}
            mid = args.chain
            for _ in range(8):
                m = ctx.api.get_message(args.channel, mid)
                chain.append(m)
                author = m.get("author")
                if author and author not in users:
                    users[author] = ctx.api.get_user(author)
                if not m.get("replies"):
                    break
                mid = m["replies"][0]
            msgs = list(reversed(chain))
        elif args.around:
            msgs, users = ctx.api.fetch_messages(args.channel, limit=args.limit,
                                                 nearby=args.around)
            msgs = sorted(msgs, key=lambda m: m["_id"])
        else:
            msgs, users = ctx.api.fetch_messages(args.channel, limit=args.limit, sort="Latest")
            msgs = sorted(msgs, key=lambda m: m["_id"])
    except Exception as e:
        # Never let a failed read look like an empty channel.
        emit({"error": "history-unavailable", "detail": str(e)[:300]}, sys.stderr)
        return 2
    for m in msgs:
        emit(_fmt_message(m, users, me))
    return 0


def cmd_send(ctx, args):
    try:
        ctx.check_msg(args.msg_id)
    except WrongRoute as e:
        return wrong_route(e)
    ctx.touch_lease()
    # With attachments the caption is optional: don't block on a terminal.
    text = "" if args.attach and sys.stdin.isatty() else sys.stdin.read()
    if text.strip().startswith("--"):
        emit({"result": "looks-like-a-flag", "exit": 1,
              "hint": "the body must come from stdin, not from a command-line flag"})
        return 1
    if args.mention:
        text = " ".join(f"<@{u}>" for u in args.mention) + " " + text
    part = None
    if args.part:
        try:
            k, n = (int(x) for x in args.part.split("/"))
            if not 1 <= k <= n:
                raise ValueError
            part = (k, n)
        except ValueError:
            emit({"result": "bad-part", "exit": 1, "hint": "use --part k/n, 1 <= k <= n"})
            return 1
    reply_to = {"auto": "auto", "none": None}.get(args.reply_to, args.reply_to)
    try:
        outcome = ctx.delivery().send(args.msg_id, text, part=part, reply_to=reply_to,
                                      mention_reply=args.mention_reply,
                                      attachments=args.attach)
    except StateCorruptError as e:
        emit({"result": "state-corrupt", "exit": FAIL_CLOSED, "detail": str(e)})
        return FAIL_CLOSED
    except ValueError as e:
        emit({"result": "bad-msg-id", "exit": 1, "detail": str(e)})
        return 1
    emit(outcome.as_dict())
    return outcome.code


def cmd_media_limits(ctx, args):
    """What `send --attach` accepts, so a skill can resize before sending."""
    limits = ctx.api.media_limits()
    # Generated files belong inside the installation, never in /tmp.
    work_dir = ctx.cfg.path("media")
    os.makedirs(work_dir, exist_ok=True)
    emit({**limits, "types": sorted(ctx.cfg.attach_types), "work_dir": work_dir,
          "formats": {"image": ["png", "jpeg", "gif", "webp", "avif", "heic"],
                      "video": ["mp4", "mov", "webm", "mkv", "avi"]}})
    return 0


def cmd_no_reply(ctx, args):
    try:
        ctx.check_msg(args.msg_id)
    except WrongRoute as e:
        return wrong_route(e)
    ctx.touch_lease()
    try:
        outcome = ctx.delivery().no_reply(args.msg_id, args.reason)
    except StateCorruptError as e:
        emit({"result": "state-corrupt", "exit": FAIL_CLOSED, "detail": str(e)})
        return FAIL_CLOSED
    emit(outcome.as_dict())
    return outcome.code


def cmd_typing(ctx, args):
    try:
        ctx.check_channel(args.channel)
    except WrongRoute as e:
        return wrong_route(e)
    ctx.touch_lease()
    path = ctx.cfg.path("typing")
    os.makedirs(path, exist_ok=True)
    write_json(os.path.join(path, f"{args.channel}.json"),
               {"channel": args.channel, "op": args.op, "at": time.time()})
    return 0


def cmd_lease(ctx, args):
    if args.release:
        ctx.drop_lease()
    elif args.hold is not None:
        secs = max(0, min(args.hold, LEASE_MAX_HOLD))
        lease = ctx.touch_lease(hold_until=time.time() + secs)
        emit({"result": "held", "hold_secs": secs, "hold_until": lease.get("hold_until")})
    else:
        ctx.touch_lease()
    return 0


def _held_by_send_state(ctx):
    """msg_ids with an unfinished send attempt (uncertain, retry_wait, ...): the
    worker cannot finish them now, and retrying would not help."""
    try:
        sends = SendState(ctx.cfg.path("send_state.json")).snapshot()
    except StateCorruptError:
        return None
    return {key.split("#", 1)[0] for key in sends}


def cmd_warm_wait(ctx, args):
    lease = ctx.touch_lease()
    cap = lease["since"] + args.total
    deadline = min(time.time() + args.idle, cap)
    # Not new: messages held by the send state machine, and messages this warm
    # session was already given once (the worker saw them and left them; the
    # hook re-wakes for them later). Anything else that is pending, including a
    # message that landed after the worker's last peek, is new right now.
    held = _held_by_send_state(ctx)
    while True:
        now = time.time()
        pending = {r["msg_id"] for r in ctx.pending()}
        skip = set((ctx.lease() or {}).get("offered", [])) | (pending if held is None else held)
        fresh = pending - skip
        if fresh:
            ctx.touch_lease("busy", offered=fresh)
            print("NEW_MESSAGES")
            return 0
        if now >= deadline:
            ctx.drop_lease()
            print("TOTAL_CAP" if now >= cap else "IDLE_TIMEOUT")
            return 0
        ctx.touch_lease("warm", until=cap)
        time.sleep(args.poll)


# --- hook ------------------------------------------------------------------------------


def _bridge_alert(ctx, state, now):
    """Edge-triggered bridge health alert: (reason, payload) or None."""
    alive = read_json(ctx.cfg.path("bridge.alive"))
    healthy = bool(alive) and now - alive.get("updated_at", 0) <= BRIDGE_STALE \
        and alive.get("connected")
    if healthy:
        since, alerted = state.pop("bridge_down_since", None), state.pop("bridge_alert_last", None)
        if alerted and since:
            return "bridge_recovered", {"down_minutes": int((now - since) / 60)}
        return None
    since = state.setdefault("bridge_down_since", now)
    last = state.get("bridge_alert_last", 0)
    if now - since >= BRIDGE_ALERT_AFTER and now - last >= ALERT_REPEAT:
        state["bridge_alert_last"] = now
        return "bridge_down", {"down_minutes": int((now - since) / 60), "repeat": bool(last),
                               "bridge_alive": alive}
    return None


def _ctl_command(ctx):
    # Everything a worker shell needs, explicitly: it does not inherit the env
    # the hook wrapper exported.
    cmd = " ".join(shlex.quote(x) for x in (
        sys.executable, os.path.join(BASE, "stoatctl.py"),
        "--data-dir", ctx.cfg.data_dir, "--routes-file", ctx.cfg.routes_file))
    return cmd + (f" --route {shlex.quote(ctx.route)}" if ctx.route else "")


def _route_payload(ctx):
    if not ctx.route:
        return {}
    info = ctx.routes.info(ctx.route)
    out = {"route": ctx.route, "side_chat": info["side_chat"]}
    if info.get("notes"):
        out["route_notes"] = info["notes"]
    return out


def _unrouted(ctx, channels, by_id, eligible, directory):
    """Channels that reached the catch-all only as a fallback, with the reason,
    so the worker can tell a human how to give them their own side chat."""
    out = []
    for cid, c in channels.items():
        rec = next(by_id[i] for i in eligible if by_id[i]["channel"] == cid)
        found = ctx.routes.candidates(cid, rec.get("channel_type"), rec.get("server"))
        hint = suggested_name(directory.get(cid) or c, cid)
        if not found:
            reason = ("auto side chats are off for this channel type / server, "
                      "or the auto caps are reached"
                      if ctx.routes.auto["mode"] == "auto" else "no route matches")
            out.append({"channel": cid, "name": c["name"], "reason": reason,
                        "suggest": f"stoatctl routes add {hint} {cid}"})
        elif any(ctx.routes.status(n) == "failed" for n in found):
            name = next(n for n in found if ctx.routes.status(n) == "failed")
            out.append({"channel": cid, "name": c["name"], "route": name,
                        "reason": ctx.routes.routes[name].get("failed_reason"),
                        "suggest": f"register hook stoat-{name} in Muse "
                                   f"(it activates on its first run)"})
    return out


def _activate_on_first_run(ctx):
    """A route hook that runs proves Muse registered it: pending -> active.
    A late registration also revives an auto route that timed out."""
    if not ctx.route or ctx.routes.status(ctx.route) == "active":
        return
    with locked_routes(ctx.cfg) as routes:
        if ctx.route in routes.routes and routes.status(ctx.route) != "active" \
                and (routes.status(ctx.route) == "pending" or routes.routes[ctx.route].get("auto")):
            routes.activate(ctx.route)
    ctx.routes = Routes.load(ctx.cfg)
    ctx.routes_mtime = _mtime(ctx.cfg.routes_file)


def hook_decision(ctx, window, dry_run):
    state_path = ctx._file("hook_state", "json")
    state = read_json(state_path, {})
    if not dry_run:
        _activate_on_first_run(ctx)
    now = time.time()
    # Bridge health is reported by one route only, so N channels do not raise N alerts.
    reports_alerts = ctx.route is None or ctx.route == ctx.routes.alerts_route
    alert = _bridge_alert(ctx, state, now) if reports_alerts else None
    deadline = now + (0 if dry_run else window)
    directory = load_directory(ctx.cfg)
    decision = None
    while decision is None:
        now = time.time()
        # routes.json may change while this run waits (a channel just got its
        # own side chat); waking on a stale mapping would wake two hooks.
        if _mtime(ctx.cfg.routes_file) != ctx.routes_mtime:
            ctx.routes_mtime = _mtime(ctx.cfg.routes_file)
            ctx.routes = Routes.load(ctx.cfg)
            directory = load_directory(ctx.cfg)
            if ctx.route and ctx.route not in ctx.routes.routes:
                decision = ("silent", f"route {ctx.route!r} was removed", None)
                break
        pending = ctx.pending()
        if pending:
            if ctx.lease_active(ctx.lease(), now):
                decision = ("silent", "worker on duty", None)
                break
            ids = [r["msg_id"] for r in pending]
            counts = {k: v for k, v in (state.get("wake_counts") or {}).items() if k in ids}
            eligible = [i for i in ids if counts.get(i, 0) < MAX_WAKES_PER_MESSAGE]
            stuck = [i for i in ids if counts.get(i, 0) >= MAX_WAKES_PER_MESSAGE]
            recent = set(state.get("last_wake_ids") or []) \
                if now - state.get("last_wake_at", 0) < WAKE_COOLDOWN else set()
            if any(i not in recent for i in eligible):
                by_id = {r["msg_id"]: r for r in pending}
                channels, kinds = {}, {}
                for i in eligible:
                    r = by_id[i]
                    entry = channels.setdefault(r["channel"], {
                        "name": (directory.get(r["channel"]) or {}).get("name")
                        or r.get("channel_name"),
                        "channel_type": r.get("channel_type"), "count": 0})
                    entry["count"] += 1
                    kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
                payload = {
                    **_route_payload(ctx),
                    "count": len(eligible), "msg_ids": eligible[:20],
                    "channels": channels, "kinds": kinds,
                    "oldest_age_secs": int(now - by_id[eligible[0]]["received_at"]),
                    "listen_reply": ctx.cfg.listen_reply,
                    "ctl": _ctl_command(ctx),
                }
                if stuck:
                    payload["stuck_msg_ids"] = stuck[:10]
                if ctx.route and ctx.route == ctx.routes.default_route \
                        and (len(ctx.routes.routes) > 1 or ctx.routes.auto["mode"] == "auto"):
                    unrouted = _unrouted(ctx, channels, by_id, eligible, directory)
                    if unrouted:
                        payload["unrouted"] = unrouted
                if alert:
                    payload["alert"] = {"type": alert[0], **alert[1]}
                for i in eligible:
                    counts[i] = counts.get(i, 0) + 1
                state.update(wake_counts=counts, last_wake_ids=eligible, last_wake_at=now)
                decision = ("wake", "new_stoat_messages", payload)
                break
            state["wake_counts"] = counts
        if now >= deadline:
            break
        time.sleep(0.5)
    if decision is None:
        if alert:
            decision = ("wake", alert[0], dict(alert[1], **_route_payload(ctx),
                                               ctl=_ctl_command(ctx)))
        else:
            decision = ("silent", "no new messages", None)
    if not dry_run:
        write_json(state_path, state)
    return decision


def _provision(ctx, routes, now):
    """Fail expired pending routes; create pending routes for AWAITING channels."""
    timeout = routes.auto["pending_timeout_secs"]
    failed = []
    for name in routes.expired_pending(now):
        routes.fail(name, f"hook not registered in Muse within {timeout}s", now)
        failed.append(name)
    directory = load_directory(ctx.cfg)
    awaiting = {}
    for rec in ctx.inbox.pending():
        if routes.route_for_record(rec, now) == AWAITING:
            awaiting.setdefault(rec["channel"], rec)
    created = []
    for cid, rec in awaiting.items():
        ctype = rec.get("channel_type")
        if not routes.auto_eligible(ctype, rec.get("server"), now):
            continue  # a cap was reached part-way through this batch
        label = (directory.get(cid) or {}).get("name") or rec.get("channel_name")
        if not label and ctype == "DirectMessage" and rec.get("author_name"):
            label = f"dm-{rec['author_name']}"
        label = label or cid[-8:]
        created.append(routes.provision(cid, label, label, ctype, now))
    return created, failed


def admin_decision(ctx, window, dry_run):
    """The admin hook: provision per-channel routes and ask the admin worker to
    register their hooks in Muse."""
    state_path = ctx.cfg.path("hook_state.admin.json")
    state = read_json(state_path, {})
    announced = state.setdefault("announced", {})
    deadline = time.time() + (0 if dry_run else window)
    while True:
        now = time.time()
        if dry_run:
            routes = Routes.load(ctx.cfg)
            created, failed = _provision(ctx, routes, now)
        else:
            with locked_routes(ctx.cfg) as routes:
                created, failed = _provision(ctx, routes, now)
            if created or failed:
                hook_definitions(ctx.cfg, routes, write=True)
        if routes.auto["mode"] != "auto":
            return "silent", "auto side chats are off", None
        # Re-announce pending routes the admin worker has not registered yet
        # (e.g. it crashed before hooks.add), a bounded number of times.
        retry = [n for n in routes.names()
                 if routes.status(n) == "pending" and n not in created
                 and len(announced.get(n, [])) < PROVISION_ANNOUNCE_MAX
                 and now - max(announced.get(n, [0])) >= PROVISION_RETRY_AFTER]
        to_register = created + retry
        if to_register or failed:
            defs = {d["route"]: d for d in hook_definitions(ctx.cfg, routes)
                    if d.get("route")}
            payload = {"ctl": _ctl_command(ctx),
                       "pending_timeout_secs": routes.auto["pending_timeout_secs"]}
            if to_register:
                payload["register"] = [dict(defs[n], channel=routes.routes[n]["match"][0],
                                            retry=n in retry) for n in to_register]
            if failed:
                payload["failed"] = [{"route": n, "side_chat": routes.routes[n]["side_chat"],
                                      "reason": routes.routes[n].get("failed_reason")}
                                     for n in failed]
            if not dry_run:
                for n in to_register:
                    announced.setdefault(n, []).append(now)
                state["announced"] = {n: t for n, t in announced.items()
                                      if n in routes.routes and routes.status(n) == "pending"}
                write_json(state_path, state)
            reason = "provision_routes" if to_register else "routes_provision_failed"
            return "wake", reason, payload
        if now >= deadline:
            return "silent", "nothing to provision", None
        time.sleep(0.5)


# --- bridge process ---------------------------------------------------------------------

AUTOSTART_EVERY = 30  # seconds between autostart attempts while the bridge is down


def bridge_running(cfg):
    """True while some process holds <data>/bridge.lock (see bridge.py)."""
    with open(cfg.path("bridge.lock"), "a+") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(f, fcntl.LOCK_UN)
        return False


def start_bridge(cfg):
    """Start bridge.py detached from the caller (own session, no tty), logging
    to <data>/bridge.log. Returns the pid."""
    env = dict(os.environ, STOAT_DATA_DIR=cfg.data_dir, STOAT_ROUTES_FILE=cfg.routes_file,
               STOAT_HOOKS_DIR=cfg.hooks_dir, PYTHONUNBUFFERED="1")
    env.pop("HATCH_HOOK_DRY_RUN", None)
    with open(os.devnull, "rb") as devnull, open(cfg.path("bridge.log"), "ab") as log_file:
        proc = subprocess.Popen([sys.executable, os.path.join(BASE, "bridge.py")], cwd=BASE,
                                env=env, stdin=devnull, stdout=subprocess.DEVNULL,
                                stderr=log_file, start_new_session=True)
    return proc.pid


def ensure_bridge(ctx):
    """Hook-side supervisor: with STOAT_AUTOSTART on, start the bridge when it
    is not running, at most once per AUTOSTART_EVERY seconds."""
    if not ctx.cfg.autostart or os.path.exists(os.path.join(BASE, ".uninstalling")) \
            or bridge_running(ctx.cfg):
        return None
    stamp = ctx.cfg.path("bridge.autostart")
    try:
        if time.time() - os.path.getmtime(stamp) < AUTOSTART_EVERY:
            return None
    except OSError:
        pass
    with open(stamp, "w") as f:
        f.write(str(time.time()))
    return start_bridge(ctx.cfg)


def _bridge_pid(cfg):
    pid = (read_json(cfg.path("bridge.alive")) or {}).get("pid")
    if not pid:
        return None
    try:
        cmd = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True,
                             text=True).stdout
    except OSError:
        return None
    return pid if "bridge.py" in cmd else None


def cmd_bridge(ctx, args):
    cfg = ctx.cfg
    if args.op == "status":
        emit({"running": bridge_running(cfg), "pid": _bridge_pid(cfg),
              "autostart": cfg.autostart, "log": cfg.path("bridge.log")})
        return 0
    if args.op == "start":
        if bridge_running(cfg):
            emit({"result": "already-running", "pid": _bridge_pid(cfg)})
            return 0
        pid = start_bridge(cfg)
        for _ in range(50):
            time.sleep(0.2)
            if bridge_running(cfg):
                emit({"result": "started", "pid": pid, "log": cfg.path("bridge.log")})
                return 0
        emit({"result": "failed-to-start", "log": cfg.path("bridge.log")})
        return 1
    # stop
    if not bridge_running(cfg):
        emit({"result": "not-running"})
        return 0
    pid = _bridge_pid(cfg)
    if pid is None:
        emit({"result": "unknown-pid", "hint": "the lock is held but bridge.alive has no "
                                               "live bridge.py pid; stop it by hand"})
        return 1
    os.kill(pid, signal.SIGTERM)
    for _ in range(100):
        time.sleep(0.2)
        if not bridge_running(cfg):
            emit({"result": "stopped", "pid": pid})
            return 0
    emit({"result": "still-running", "pid": pid})
    return 1


def cmd_hook_poll(ctx, args):
    dry_run = os.environ.get("HATCH_HOOK_DRY_RUN") == "1"
    if not dry_run:
        ensure_bridge(ctx)
    role = args.role or os.environ.get("STOAT_HOOK_ROLE") or "route"
    if role == "admin":
        lock = open(ctx.cfg.path("hook.admin.lock"), "a+")
        if not dry_run:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print("silent\tprevious admin run still active\t")
                return 0
        decision, reason, payload = admin_decision(ctx, args.window, dry_run)
        print(f"{decision}\t{reason}\t"
              f"{json.dumps(payload, ensure_ascii=False) if payload else ''}")
        return 0
    lock = open(ctx._file("hook", "lock"), "a+")
    try:
        # A dry run writes no state, so it may run beside a live run and show
        # the decision that run would make right now.
        if not dry_run:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("silent\tprevious hook run still active\t")
        return 0
    decision, reason, payload = hook_decision(ctx, args.window, dry_run)
    print(f"{decision}\t{reason}\t{json.dumps(payload, ensure_ascii=False) if payload else ''}")
    return 0


# --- operator ----------------------------------------------------------------------------


def _route_overview(ctx, now):
    out = {}
    for name in ctx.routes.names():
        out[name] = {"side_chat": ctx.routes.routes[name]["side_chat"],
                     "status": ctx.routes.status(name),
                     "pending": len(ctx.pending(name)),
                     "worker_on_duty": ctx.lease_active(ctx.lease(name), now)}
    return out


def cmd_status(ctx, args):
    now = time.time()
    alive = read_json(ctx.cfg.path("bridge.alive"))
    pending = ctx.inbox.pending()
    try:
        sends = SendState(ctx.cfg.path("send_state.json")).snapshot()
        send_summary = {k: e["status"] for k, e in sends.items()}
    except StateCorruptError as e:
        send_summary = {"error": str(e)}
    stuck = []
    for name in [None] + ctx.routes.names():
        hook = read_json(ctx._file("hook_state", "json", name), {}) if name \
            else read_json(ctx.cfg.path("hook_state.json"), {})
        stuck += [k for k, v in (hook.get("wake_counts") or {}).items()
                  if v >= MAX_WAKES_PER_MESSAGE]
    healthy = bool(alive) and alive.get("connected") \
        and now - alive.get("updated_at", 0) <= BRIDGE_STALE
    emit({
        "healthy": healthy,
        "data_dir": ctx.cfg.data_dir,
        "routes_file": ctx.routes.path if os.path.exists(ctx.routes.path) else None,
        "bridge": dict(alive, age_secs=int(now - alive.get("updated_at", 0))) if alive else None,
        "inbox_pending": len(pending),
        "oldest_pending_secs": int(now - pending[0]["received_at"]) if pending else None,
        "routes": _route_overview(ctx, now),
        "auto": ctx.routes.auto["mode"],
        "awaiting_side_chat": len(ctx.pending(AWAITING)),
        "sends": send_summary,
        "stuck_msg_ids": sorted(set(stuck)),
    })
    return 0 if healthy else 1


def cmd_sends(ctx, args):
    for key, entry in SendState(ctx.cfg.path("send_state.json")).snapshot().items():
        emit(dict(entry, key=key))
    return 0


def cmd_release(ctx, args):
    if not args.confirm_not_sent:
        emit({"error": "refusing: confirm in Stoat that the message was NOT stored, "
                       "then pass --confirm-not-sent"}, sys.stderr)
        return 1
    entry = ctx.delivery().release(args.key)
    emit({"released": args.key, "entry": entry})
    return 0 if entry else 1


def cmd_compact(ctx, args):
    kept, dropped = ctx.inbox.compact()
    emit({"kept": kept, "dropped": dropped})
    return 0


def cmd_whoami(ctx, args):
    emit({"bot_user_id": bot_identity(ctx.cfg, ctx.api), "api": ctx.cfg.api_url})
    return 0


# --- routes (operator) ------------------------------------------------------------------


def cmd_routes_list(ctx, args):
    now = time.time()
    directory = load_directory(ctx.cfg)
    overview = _route_overview(ctx, now)
    members = {}
    for cid, info in directory.items():
        r = ctx.routes.route_for(cid, info.get("channel_type"), info.get("server"))
        members.setdefault(r, []).append({"channel": cid, "name": info.get("name")})
    for name in ctx.routes.names():
        route = ctx.routes.routes[name]
        script = os.path.join(ctx.cfg.hooks_dir, f"{name}.sh")
        emit({"route": name, "side_chat": route["side_chat"], "match": route["match"],
              "default": name == ctx.routes.default_route,
              "alerts": name == ctx.routes.alerts_route,
              "notes": route.get("notes"), "known_channels": members.get(name, []),
              "hook_script": script if os.path.exists(script) else None,
              **overview[name]})
    return 0


def cmd_routes_discover(ctx, args):
    directory = load_directory(ctx.cfg)
    if not directory:
        emit({"error": "no channel directory yet; start bridge.py so it can read Ready"},
             sys.stderr)
        return 1
    for cid, info in sorted(directory.items(), key=lambda kv: (kv[1].get("channel_type") or "",
                                                               kv[1].get("name") or "")):
        route = ctx.routes.route_for(cid, info.get("channel_type"), info.get("server"))
        emit({"channel": cid, "name": info.get("name"), "type": info.get("channel_type"),
              "server": info.get("server_name"), "route": route,
              "dedicated": route != ctx.routes.default_route,
              "suggested_route": suggested_name(info, cid)})
    return 0


def _routes_saved(ctx, changed, hint=None):
    emit({"saved": ctx.cfg.routes_file, "changed": changed,
          "next": hint or "run `stoatctl routes hooks --write`, then register each new hook "
                          "in Muse with its own side chat"})
    return 0


def cmd_routes_add(ctx, args):
    with locked_routes(ctx.cfg) as routes:
        routes.add(args.name, args.selectors, side_chat=args.side_chat, notes=args.notes)
    return _routes_saved(ctx, [args.name])


def cmd_routes_add_each(ctx, args):
    directory = load_directory(ctx.cfg)
    if not directory:
        emit({"error": "no channel directory yet; start bridge.py first"}, sys.stderr)
        return 1
    added = []
    with locked_routes(ctx.cfg) as routes:
        for cid, info in sorted(directory.items()):
            ctype = info.get("channel_type")
            if cid in ctx.cfg.ignore_channels or (ctype == "DirectMessage" and not args.dms):
                continue
            if not routes.is_unrouted(cid, ctype, info.get("server")):
                continue  # already routed somewhere explicit
            name, n = suggested_name(info, cid), 2
            while name in routes.routes:
                name, n = f"{suggested_name(info, cid)}-{n}", n + 1
            label = info.get("name") or cid[-8:]
            side_chat = f"{args.prefix}{label}" if ctype == "DirectMessage" \
                else f"{args.prefix}#{label}"
            routes.add(name, [cid], side_chat=side_chat)
            added.append(name)
    return _routes_saved(ctx, added)


def cmd_routes_remove(ctx, args):
    with locked_routes(ctx.cfg) as routes:
        routes.remove(args.name)
    return _routes_saved(ctx, [args.name], "also remove hook stoat-%s from Muse" % args.name)


def cmd_routes_activate(ctx, args):
    with locked_routes(ctx.cfg) as routes:
        if args.name not in routes.routes:
            raise RouteError(f"unknown route {args.name!r}")
        routes.activate(args.name)
    return _routes_saved(ctx, [args.name], "its hook must be registered in Muse")


def cmd_routes_fail(ctx, args):
    with locked_routes(ctx.cfg) as routes:
        if args.name not in routes.routes or args.name == routes.default_route:
            raise RouteError(f"cannot fail route {args.name!r}")
        routes.fail(args.name, args.reason)
    return _routes_saved(ctx, [args.name], "its channel now falls back to the default route")


def cmd_routes_auto(ctx, args):
    """Show or change the auto side chat policy."""
    with locked_routes(ctx.cfg) as routes:
        a = routes.auto
        if args.mode:
            a["mode"] = args.mode
        if args.types is not None:
            a["types"] = [t for t in args.types.split(",") if t]
        if args.servers is not None:
            a["servers"] = [t for t in args.servers.split(",") if t]
        for key in ("max_routes", "max_per_hour", "pending_timeout_secs"):
            if getattr(args, key) is not None:
                a[key] = getattr(args, key)
        if args.admin_side_chat:
            a["admin_side_chat"] = args.admin_side_chat
        policy = dict(a)
    emit({"auto": policy, "routes_file": ctx.cfg.routes_file})
    if policy["mode"] == "auto":
        emit({"next": "run `stoatctl routes hooks --write` and register hook stoat-admin "
                      "(instructions: hook/ADMIN.md) in Muse"}, sys.stderr)
    return 0


def cmd_routes_hooks(ctx, args):
    for d in hook_definitions(ctx.cfg, ctx.routes, args.poll, args.timeout, write=args.write):
        emit(d)
    if args.write:
        emit({"written": ctx.cfg.hooks_dir}, sys.stderr)
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="stoatctl", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", help="override STOAT_DATA_DIR")
    p.add_argument("--routes-file", help="override STOAT_ROUTES_FILE")
    p.add_argument("--route", help="scope to one route / side chat (default: STOAT_ROUTE)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("peek")
    s.add_argument("--count", action="store_true")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(fn=cmd_peek)

    s = sub.add_parser("history")
    s.add_argument("channel")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--around")
    s.add_argument("--chain")
    s.set_defaults(fn=cmd_history)

    s = sub.add_parser("send")
    s.add_argument("msg_id")
    s.add_argument("--part")
    s.add_argument("--reply-to", default="auto")
    s.add_argument("--mention-reply", action="store_true")
    s.add_argument("--mention", action="append", default=[])
    s.add_argument("--attach", action="append", default=[], metavar="FILE",
                   help="image/video file to upload with this message (repeatable)")
    s.set_defaults(fn=cmd_send)

    s = sub.add_parser("media-limits")
    s.set_defaults(fn=cmd_media_limits)

    s = sub.add_parser("no-reply")
    s.add_argument("msg_id")
    s.add_argument("--reason", required=True)
    s.set_defaults(fn=cmd_no_reply)

    s = sub.add_parser("typing")
    s.add_argument("channel")
    s.add_argument("op", choices=["start", "stop"])
    s.set_defaults(fn=cmd_typing)

    s = sub.add_parser("lease")
    s.add_argument("--release", action="store_true")
    s.add_argument("--hold", type=int, metavar="SECS",
                   help=f"keep the busy lease without heartbeats (max {LEASE_MAX_HOLD}s)")
    s.set_defaults(fn=cmd_lease)

    s = sub.add_parser("warm-wait")
    s.add_argument("--idle", type=float, default=float(os.environ.get("STOAT_WARM_IDLE", 180)))
    s.add_argument("--total", type=float, default=float(os.environ.get("STOAT_WARM_TOTAL", 1800)))
    s.add_argument("--poll", type=float, default=2.0)
    s.set_defaults(fn=cmd_warm_wait)

    s = sub.add_parser("hook-poll")
    s.add_argument("--window", type=float, default=25.0)
    s.add_argument("--role", choices=["route", "admin"],
                   help="admin = provision per-channel side chats (default: STOAT_HOOK_ROLE)")
    s.set_defaults(fn=cmd_hook_poll)

    for name, fn in (("status", cmd_status), ("sends", cmd_sends),
                     ("compact", cmd_compact), ("whoami", cmd_whoami)):
        sub.add_parser(name).set_defaults(fn=fn)

    s = sub.add_parser("bridge", help="run bridge.py in the background")
    s.add_argument("op", choices=["start", "stop", "status"])
    s.set_defaults(fn=cmd_bridge)

    s = sub.add_parser("release")
    s.add_argument("key")
    s.add_argument("--confirm-not-sent", action="store_true")
    s.set_defaults(fn=cmd_release)

    routes = sub.add_parser("routes", help="channel -> side chat mapping").add_subparsers(
        dest="routes_cmd", required=True)
    routes.add_parser("list").set_defaults(fn=cmd_routes_list)
    routes.add_parser("discover").set_defaults(fn=cmd_routes_discover)
    s = routes.add_parser("add", help="route NAME gets the given selectors")
    s.add_argument("name")
    s.add_argument("selectors", nargs="*",
                   help="channel id | server:<id> | type:<DirectMessage|Group|TextChannel>")
    s.add_argument("--side-chat")
    s.add_argument("--notes")
    s.set_defaults(fn=cmd_routes_add)
    s = routes.add_parser("add-each", help="one route per known channel not yet routed")
    s.add_argument("--no-dms", dest="dms", action="store_false",
                   help="leave direct messages on the default route")
    s.add_argument("--prefix", default="Stoat · ", help="side chat name prefix")
    s.set_defaults(fn=cmd_routes_add_each)
    s = routes.add_parser("remove")
    s.add_argument("name")
    s.set_defaults(fn=cmd_routes_remove)
    s = routes.add_parser("activate", help="mark a route active (its hook is registered)")
    s.add_argument("name")
    s.set_defaults(fn=cmd_routes_activate)
    s = routes.add_parser("fail", help="give up on a route; its channel falls back")
    s.add_argument("name")
    s.add_argument("--reason", required=True)
    s.set_defaults(fn=cmd_routes_fail)
    s = routes.add_parser("auto", help="show / change the auto side chat policy")
    s.add_argument("--mode", choices=["off", "auto"])
    s.add_argument("--types", help="comma list: TextChannel,Group,DirectMessage")
    s.add_argument("--servers", help="comma list of server ids ('' = any)")
    s.add_argument("--max-routes", dest="max_routes", type=int)
    s.add_argument("--max-per-hour", dest="max_per_hour", type=int)
    s.add_argument("--timeout", dest="pending_timeout_secs", type=int)
    s.add_argument("--admin-side-chat")
    s.set_defaults(fn=cmd_routes_auto)
    s = routes.add_parser("hooks", help="Muse hook definitions, one per route")
    s.add_argument("--write", action="store_true", help="also write hook scripts + hooks.json")
    s.add_argument("--poll", type=int, default=5)
    s.add_argument("--timeout", type=int, default=60)
    s.set_defaults(fn=cmd_routes_hooks)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    overrides = {}
    if args.data_dir:
        overrides["STOAT_DATA_DIR"] = args.data_dir
    if args.routes_file:
        overrides["STOAT_ROUTES_FILE"] = args.routes_file
    cfg = Config(overrides)
    try:
        ctx = Ctx(cfg, args.route or cfg.route)
        return args.fn(ctx, args)
    except RouteError as e:
        emit({"result": "route-config-error", "exit": FAIL_CLOSED, "detail": str(e)})
        return FAIL_CLOSED


if __name__ == "__main__":
    sys.exit(main())
