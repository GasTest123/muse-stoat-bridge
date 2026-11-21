#!/usr/bin/env python3
"""Stoat -> inbox.jsonl bridge (receive only).

Holds one Bonfire WebSocket connection, decides which messages are
addressed to the bot (src/filters.py) and appends them to inbox.jsonl. Replies
never go through here; the agent sends with `stoatctl.py send`.

Bonfire has no per-event ACK and never redelivers, so durability comes from
per-channel cursors plus REST backfill:

  * every handled message advances `cursors[channel]` (flushed every 2s);
  * on each Ready, channels whose `last_message_id` is past the cursor are
    backfilled with GET /channels/{id}/messages?after=<cursor>;
  * a message that fails to classify or persist marks its channel dirty:
    the cursor stops moving and a backfill retries from the old cursor.
Overlap between WebSocket delivery and backfill is absorbed by msg_id dedupe.
"""
import asyncio
import fcntl
import json
import logging
import os
import signal
import ssl
import sys
import time
from collections import Counter, OrderedDict

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "src"))  # library modules live in src/

from websockets.asyncio.client import connect  # noqa: E402

import ulidutil  # noqa: E402
from config import Config  # noqa: E402
from delivery import bot_identity  # noqa: E402
from filters import decide  # noqa: E402
from inbox_store import Inbox  # noqa: E402
from stoat_api import ApiError, StoatAPI  # noqa: E402

PING_INTERVAL = 20
PONG_TIMEOUT = 75
FLUSH_INTERVAL = 2
ALIVE_INTERVAL = 5
DIRTY_RETRY_INTERVAL = 30
COMPACT_INTERVAL = 6 * 3600
STATS_INTERVAL = 30
OWN_IDS_CAP = 5000

log = logging.getLogger("muse-stoat-bridge")


class FatalAuthError(Exception):
    pass


def _atomic_json(path, obj):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _read_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


class Bridge:
    def __init__(self, cfg):
        self.cfg = cfg
        self.api = StoatAPI(cfg)
        self.inbox = Inbox(cfg.path("inbox.jsonl"))
        self.cursor_path = cfg.path("cursors.json")
        self.own_path = cfg.path("own_messages.json")
        self.alive_path = cfg.path("bridge.alive")
        self.typing_dir = cfg.path("typing")
        state = _read_json(self.cursor_path, None)
        self.first_run = state is None
        state = state or {}
        self.cursors = dict(state.get("channels") or {})
        self.last_alive_ms = state.get("last_alive_ms")
        self.own_ids = OrderedDict((i, True) for i in _read_json(self.own_path, []))
        self.not_own = set()
        self.channels = {}
        self.servers = {}
        self.users = {}
        self.directory_path = cfg.path("channel_directory.json")
        self._directory_changed = False
        self.autumn_url = None
        self.me = None
        self.dirty = set()
        self.failures = {}  # channel -> count of live-event failures
        self.stats = Counter()  # message decisions since the last summary line
        self.backfilling = set()
        self._state_changed = False
        self.stop = asyncio.Event()  # recreated in main(); usable without it too
        self.ws = None
        self.connected_since = None
        self.last_event_at = None
        self.last_pong = None

    # --- persistence -------------------------------------------------------------

    def flush_state(self):
        if not self._state_changed:
            return
        self._state_changed = False
        _atomic_json(self.cursor_path, {"channels": self.cursors,
                                        "last_alive_ms": self.last_alive_ms})
        _atomic_json(self.own_path, list(self.own_ids)[-OWN_IDS_CAP:])

    def flush_directory(self):
        """Channel directory for `stoatctl routes` (names, types, servers)."""
        if not self._directory_changed:
            return
        self._directory_changed = False
        out = {}
        for cid, ch in self.channels.items():
            ctype = ch.get("channel_type")
            if ctype == "SavedMessages":
                continue
            name = ch.get("name")
            if ctype == "DirectMessage":
                others = [r for r in ch.get("recipients") or [] if r != self.me]
                user = self.users.get(others[0], {}) if others else {}
                name = "dm-" + (user.get("username") or (others[0][-6:] if others else cid[-6:]))
            server = ch.get("server")
            out[cid] = {"name": name, "channel_type": ctype, "server": server,
                        "server_name": self.servers.get(server, {}).get("name") if server else None,
                        "last_message_id": ch.get("last_message_id")}
        _atomic_json(self.directory_path, out)

    def write_alive(self):
        if self.connected_since:
            self.last_alive_ms = int(time.time() * 1000)
            self._state_changed = True
        _atomic_json(self.alive_path, {
            "pid": os.getpid(), "updated_at": time.time(),
            "connected": self.connected_since is not None,
            "connected_since": self.connected_since,
            "last_event_at": self.last_event_at,
            "dirty_channels": sorted(self.dirty), "me": self.me})

    def _remember_own(self, message_id):
        self.own_ids[message_id] = True
        while len(self.own_ids) > OWN_IDS_CAP:
            self.own_ids.popitem(last=False)
        self._state_changed = True

    def _advance(self, channel, message_id, force=False):
        if not force and channel in self.dirty:
            return
        if message_id > self.cursors.get(channel, ""):
            self.cursors[channel] = message_id
            self._state_changed = True

    # --- lookups (sync; run in a worker thread) ------------------------------------

    def _channel_type(self, channel_id):
        ch = self.channels.get(channel_id)
        if ch is None:
            ch = self.api.get_channel(channel_id)
            self.channels[channel_id] = ch
            self._directory_changed = True
        return ch.get("channel_type")

    def _listening(self, channel_id):
        """STOAT_LISTEN_CHANNELS: channel ids, `server:<id>` (every channel of that
        server, including ones created later) or `*` (every server channel)."""
        selectors = self.cfg.listen_channels
        if not selectors:
            return False
        if "*" in selectors or channel_id in selectors:
            return True
        server = (self.channels.get(channel_id) or {}).get("server")
        return bool(server) and f"server:{server}" in selectors

    def _user(self, user_id):
        user = self.users.get(user_id)
        if user is None:
            user = self.api.get_user(user_id)
            self.users[user_id] = user
        return user

    def _reply_to_me(self, msg):
        for rid in msg.get("replies") or []:
            if rid in self.own_ids:
                return True
            if rid in self.not_own:
                continue
            try:
                parent = self.api.get_message(msg["channel"], rid)
            except ApiError as e:
                if e.status == 404:
                    self.not_own.add(rid)
                    continue
                raise
            if parent.get("author") == self.me:
                self._remember_own(rid)
                return True
            self.not_own.add(rid)
        return False

    def _record(self, msg, kind, source):
        author = self.users.get(msg["author"], {})
        channel = self.channels.get(msg["channel"], {})
        attachments = []
        for a in msg.get("attachments") or []:
            item = {k: a.get(k) for k in ("_id", "filename", "content_type", "size")}
            if self.autumn_url and a.get("_id"):
                item["url"] = f"{self.autumn_url}/{a.get('tag', 'attachments')}/{a['_id']}"
            attachments.append(item)
        return {
            "msg_id": f"{msg['channel']}:{msg['_id']}",
            "channel": msg["channel"],
            "channel_type": channel.get("channel_type"),
            "channel_name": channel.get("name"),
            "server": channel.get("server"),
            "message_id": msg["_id"],
            "author": msg["author"],
            "author_name": author.get("display_name") or author.get("username"),
            "author_is_bot": bool(author.get("bot")),
            "content": msg.get("content") or "",
            "mentions": msg.get("mentions") or [],
            "replies": msg.get("replies") or [],
            "attachments": attachments,
            "kind": kind,
            "created_at": ulidutil.time_ms(msg["_id"]) / 1000.0,
            "received_at": time.time(),
            "source": source,
        }

    def handle_message_sync(self, msg, source):
        """Classify and, if addressed to us, persist. Raises on any failure so the
        caller keeps the channel cursor where it was."""
        if msg.get("channel") in self.cfg.ignore_channels:
            action, detail = "ignore", "ignored-channel"
        else:
            if isinstance(msg.get("user"), dict):
                self.users[msg["user"]["_id"]] = msg["user"]
            action, detail = decide(
                msg, self.me,
                channel_type=self._channel_type(msg["channel"]),
                author_is_bot=lambda: bool(self._user(msg["author"]).get("bot")),
                reply_to_me=lambda: self._reply_to_me(msg),
                allowed_bots=self.cfg.allowed_bots,
                listen=self._listening(msg["channel"]))
        self.stats[f"{action}:{detail}"] += 1
        if action == "ignore":
            log.debug("ignored %s:%s from %s (%s)", msg.get("channel"), msg.get("_id"),
                      msg.get("author"), detail)
        if action == "own":
            self._remember_own(msg["_id"])
        elif action == "queue":
            if self.inbox.append(self._record(msg, detail, source)):
                log.info("queued %s %s:%s (%s)", detail, msg["channel"], msg["_id"], source)
        return action, detail

    # --- async event handling -------------------------------------------------------

    async def on_message(self, msg):
        channel = msg.get("channel")
        try:
            await asyncio.to_thread(self.handle_message_sync, msg, "ws")
        except Exception as e:
            log.warning("message %s:%s not handled (%s); channel marked for backfill",
                        channel, msg.get("_id"), e)
            self.failures[channel] = self.failures.get(channel, 0) + 1
            self.dirty.add(channel)
            return
        self._advance(channel, msg["_id"])

    async def backfill(self, channel_id, reason):
        if channel_id in self.backfilling:
            return
        self.backfilling.add(channel_id)
        self.dirty.add(channel_id)
        # A live event that fails after this point may lie past our last page,
        # so the channel must stay dirty for another round.
        failures_at_start = self.failures.get(channel_id, 0)
        floor = ulidutil.from_ms((time.time() - self.cfg.backfill_max_age) * 1000)
        after = self.cursors.get(channel_id) or None
        if after is None or after < floor:
            if after is not None:
                log.warning("backfill %s: cursor older than %ss, skipping earlier history",
                            channel_id, self.cfg.backfill_max_age)
            after = floor
        seen = 0
        try:
            while True:
                msgs, users = await asyncio.to_thread(
                    self.api.fetch_messages, channel_id, 100, None, after, "Oldest")
                self.users.update(users)
                for m in msgs:
                    await asyncio.to_thread(self.handle_message_sync, m, "backfill")
                    self._advance(channel_id, m["_id"], force=True)
                    after = m["_id"]
                    seen += 1
                if len(msgs) < 100:
                    break
            if self.failures.get(channel_id, 0) == failures_at_start:
                self.dirty.discard(channel_id)
            log.info("backfill %s done (%s): %d messages scanned", channel_id, reason, seen)
        except Exception as e:
            log.warning("backfill %s failed after %d messages: %s", channel_id, seen, e)
        finally:
            self.backfilling.discard(channel_id)

    async def backfill_many(self, channel_ids, reason):
        for cid in channel_ids:
            if self.stop.is_set():
                return
            await self.backfill(cid, reason)

    async def on_ready(self, ev):
        for u in ev.get("users") or []:
            self.users[u["_id"]] = u
        self.channels = {c["_id"]: c for c in ev.get("channels") or [] if "_id" in c}
        self.servers = {sv["_id"]: sv for sv in ev.get("servers") or [] if "_id" in sv}
        self._directory_changed = True
        since_ms = (self.last_alive_ms or 0) - 60_000
        todo = []
        for cid, ch in self.channels.items():
            if cid in self.cfg.ignore_channels or ch.get("channel_type") == "SavedMessages":
                continue
            last = ch.get("last_message_id") or ""
            if cid not in self.cursors:
                if self.first_run or not self.last_alive_ms:
                    self.cursors[cid] = last  # start from "now" on a fresh install
                    self._state_changed = True
                    continue
                # Channel appeared while we were offline (e.g. a new DM).
                self.cursors[cid] = ulidutil.from_ms(since_ms)
                self._state_changed = True
            if last > self.cursors[cid]:
                todo.append(cid)
        self.first_run = False
        log.info("ready: %d channels, %d users, %d to backfill",
                 len(self.channels), len(self.users), len(todo))
        self.dirty.update(todo)
        if todo:
            asyncio.create_task(self.backfill_many(todo, "ready"))

    async def handle_event(self, ev):
        etype = ev.get("type")
        if etype == "Bulk":
            for sub in ev.get("v") or []:
                await self.handle_event(sub)
        elif etype == "Message":
            await self.on_message(ev)
        elif etype == "Ready":
            await self.on_ready(ev)
        elif etype == "Pong":
            self.last_pong = time.time()
        elif etype == "Authenticated":
            log.info("authenticated")
        elif etype == "ChannelCreate":
            self.channels[ev["_id"]] = {k: v for k, v in ev.items() if k != "type"}
            self.cursors.setdefault(ev["_id"], "")
            self._directory_changed = True
        elif etype == "ChannelUpdate":
            self.channels.setdefault(ev.get("id"), {}).update(ev.get("data") or {})
            self._directory_changed = True
        elif etype == "ChannelDelete":
            self.channels.pop(ev.get("id"), None)
            self._directory_changed = True
        elif etype == "Error":
            err = ev.get("error")
            if err in ("InvalidSession", "NotAuthenticated"):
                raise FatalAuthError(f"Bonfire rejected credentials: {err}")
            raise ConnectionError(f"Bonfire error: {err}")
        elif etype == "Logout":
            raise ConnectionError("Bonfire sent Logout")

    # --- connection ------------------------------------------------------------------

    async def _recv_loop(self, ws):
        async for raw in ws:
            self.last_event_at = time.time()
            self.last_pong = self.last_event_at
            try:
                ev = json.loads(raw)
            except ValueError:
                continue
            await self.handle_event(ev)
        raise ConnectionError("websocket closed")

    async def _heartbeat(self, ws):
        while True:
            await asyncio.sleep(PING_INTERVAL)
            if time.time() - (self.last_pong or 0) > PONG_TIMEOUT:
                raise ConnectionError("no Pong/events within timeout")
            await ws.send(json.dumps({"type": "Ping", "data": int(time.time() * 1000)}))

    async def _typing_pump(self, ws):
        """Relay `stoatctl typing` requests; BeginTyping only exists on the socket."""
        while True:
            await asyncio.sleep(0.5)
            try:
                names = os.listdir(self.typing_dir)
            except FileNotFoundError:
                continue
            for name in names:
                if not name.endswith(".json"):
                    continue
                path = os.path.join(self.typing_dir, name)
                req = _read_json(path, None)
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
                if not req or time.time() - req.get("at", 0) > 30:
                    continue
                op = "BeginTyping" if req.get("op") == "start" else "EndTyping"
                await ws.send(json.dumps({"type": op, "channel": req["channel"]}))

    async def run_once(self):
        root = await asyncio.to_thread(self.api.root)
        autumn = ((root.get("features") or {}).get("autumn") or {})
        self.autumn_url = self.cfg.autumn_url or (
            autumn.get("url").rstrip("/") if autumn.get("enabled") and autumn.get("url") else None)
        ws_url = self.cfg.ws_url or root.get("ws")
        if not ws_url:
            raise RuntimeError("API root has no `ws` field; set STOAT_WS_URL")
        self.me = await asyncio.to_thread(bot_identity, self.cfg, self.api)
        url = ws_url + ("&" if "?" in ws_url else "?") + "version=1&format=json"
        ssl_ctx = None
        if url.startswith("wss://"):
            ssl_ctx = ssl.create_default_context(cafile=self.cfg.ca_bundle) \
                if self.cfg.ca_bundle else ssl.create_default_context()
        log.info("connecting to %s as %s", ws_url, self.me)
        async with connect(url, ssl=ssl_ctx, max_size=None, ping_interval=None,
                           open_timeout=20, close_timeout=5) as ws:
            # Token in the first frame rather than the URL keeps it out of proxy logs.
            await ws.send(json.dumps({"type": "Authenticate", "token": self.cfg.token}))
            self.ws = ws
            self.connected_since = time.time()
            self.last_pong = time.time()
            log.info("socket connected, listening")
            tasks = [asyncio.create_task(self._recv_loop(ws)),
                     asyncio.create_task(self._heartbeat(ws)),
                     asyncio.create_task(self._typing_pump(ws)),
                     asyncio.create_task(self.stop.wait())]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    if t.exception():
                        raise t.exception()
            finally:
                for t in tasks:
                    t.cancel()
                self.ws = None
                self.connected_since = None

    async def serve_forever(self, initial_backoff=2, max_backoff=300):
        backoff = initial_backoff
        while not self.stop.is_set():
            started = time.time()
            try:
                await self.run_once()
            except FatalAuthError as e:
                log.error("%s; check STOAT_BOT_TOKEN", e)
                backoff = max_backoff
            except Exception as e:
                log.warning("connection ended: %s: %s", type(e).__name__, e)
            if self.stop.is_set():
                break
            if time.time() - started > 60:
                backoff = initial_backoff
            log.info("reconnecting in %ss", backoff)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, max_backoff)

    def log_stats(self):
        """One summary line, so a quiet log never hides messages that were seen
        but deliberately not queued."""
        if not self.stats:
            return
        stats, self.stats = self.stats, Counter()
        parts = ", ".join(f"{k.replace(':', ' ')}×{v}" for k, v in sorted(stats.items()))
        hint = ""
        if stats.get("ignore:not-addressed-to-us"):
            hint = (" — server channels only queue @mentions of the bot and replies to its "
                    "messages (see STOAT_LISTEN_CHANNELS)")
        log.info("messages in last %ss: %d (%s)%s", STATS_INTERVAL, sum(stats.values()),
                 parts, hint)

    async def _maintenance(self):
        last_stats = time.time()
        last_compact = 0
        last_dirty = time.time()
        last_alive = 0
        while not self.stop.is_set():
            now = time.time()
            if now - last_alive >= ALIVE_INTERVAL:
                self.write_alive()
                last_alive = now
            self.flush_state()
            self.flush_directory()
            if now - last_stats >= STATS_INTERVAL:
                last_stats = now
                self.log_stats()
            if self.ws is not None and now - last_dirty >= DIRTY_RETRY_INTERVAL:
                last_dirty = now
                retry = [c for c in self.dirty if c not in self.backfilling]
                if retry:
                    asyncio.create_task(self.backfill_many(retry, "retry"))
            if now - last_compact >= COMPACT_INTERVAL:
                last_compact = now
                try:
                    kept, dropped = await asyncio.to_thread(self.inbox.compact)
                    if dropped:
                        log.info("inbox compacted: kept %d, dropped %d", kept, dropped)
                except Exception as e:
                    log.warning("inbox compact failed: %s", e)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=FLUSH_INTERVAL)
            except asyncio.TimeoutError:
                pass

    async def main(self):
        self.stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop.set)
        maint = asyncio.create_task(self._maintenance())
        try:
            await self.serve_forever()
        finally:
            self.stop.set()
            await maint
            self.flush_state()
            self.write_alive()
            self.log_stats()
            log.info("bridge stopped")


def acquire_instance_lock(cfg):
    """Exclusive lock on <data>/bridge.lock for the life of the process, or
    None when another bridge already holds it (one bridge per data dir)."""
    f = open(cfg.path("bridge.lock"), "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        return None
    return f


def main():
    cfg = Config()
    lock = acquire_instance_lock(cfg)
    if lock is None:
        print(f"another bridge is already running on {cfg.data_dir}", file=sys.stderr)
        return 0
    logging.basicConfig(
        level=os.environ.get("STOAT_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler(cfg.path("bridge.log"))])
    if not cfg.token:
        log.error("STOAT_BOT_TOKEN is not set (see .env.example)")
        return 2
    asyncio.run(Bridge(cfg).main())
    return 0


if __name__ == "__main__":
    sys.exit(main())
