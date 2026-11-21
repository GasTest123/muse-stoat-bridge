"""Durable delivery of one agent reply, or a recorded decision not to reply.

The flow:

  claim (fsync `sending`)  ->  POST with Idempotency-Key = attempt id
    sent        -> mark `unacked` -> write inbox tombstone -> drop entry
    not_sent    -> drop entry (clean retry next time)
    retry_wait  -> keep `retry_at`; refuse to send before it
    uncertain   -> verify: page channel history from claim time and look for
                   our own message whose `nonce` equals the attempt id

Attachments (local image / video files) are checked before the claim and
uploaded to Autumn after it, just before the POST. An upload failure means
the message was never sent (`not_sent`); ids from an abandoned attempt are
harmless orphans on the file server, so a retry simply uploads again.

A blind resend never happens: an uncertain attempt stays uncertain until
history proves it was stored, or an operator releases it after checking.

Exit codes (stoatctl returns them unchanged):
  0  done (sent+acked, quiet-completed, or already done by someone else)
  1  provably not sent; safe to retry
  2  uncertain / held by a live sender; do NOT resend, retry later to verify
  3  sent but the inbox tombstone failed; rerun only re-acks, never resends
  4  fail-closed: unknown source, identity unavailable, or corrupt state
  75 rate limited; `retry_at` persisted; do not retry before it
"""
import hashlib
import json
import os
import socket
import time
import uuid

import media
import ulidutil
from send_state import owner_alive

OK, NOT_SENT, UNCERTAIN, UNACKED, FAIL_CLOSED, RETRY_WAIT = 0, 1, 2, 3, 4, 75
MAX_LEN = 2000
VERIFY_SKEW_MS = 60_000
VERIFY_MAX_PAGES = 20


class Outcome:
    def __init__(self, code, result, **info):
        self.code = code
        self.result = result
        self.info = info

    def as_dict(self):
        return {"result": self.result, "exit": self.code, **self.info}


def split_msg_id(msg_id):
    if ":" not in msg_id:
        raise ValueError(f"msg_id must be <channel>:<message>, got {msg_id!r}")
    return msg_id.split(":", 1)


def bot_identity(cfg, api):
    """Bot user id: config, then cache, then GET /users/@me."""
    if cfg.bot_user_id:
        return cfg.bot_user_id
    path = cfg.path("identity.json")
    try:
        with open(path) as f:
            return json.load(f)["user_id"]
    except (OSError, ValueError, KeyError):
        pass
    me = api.me()
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"user_id": me["_id"], "username": me.get("username")}, f)
    os.replace(tmp, path)
    return me["_id"]


def find_attempt(api, entry, bot_user_id):
    """Our message carrying this attempt's nonce, or None. Raises on read errors
    so that a failed lookup is never mistaken for "not sent"."""
    after = ulidutil.from_ms(entry["claimed_at"] * 1000 - VERIFY_SKEW_MS)
    upper_ms = time.time() * 1000 + VERIFY_SKEW_MS
    for _ in range(VERIFY_MAX_PAGES):
        msgs, _ = api.fetch_messages(entry["channel"], limit=100, after=after,
                                     sort="Oldest", include_users=False)
        for m in msgs:
            if ulidutil.time_ms(m["_id"]) > upper_ms:
                return None
            if m.get("author") == bot_user_id and m.get("nonce") == entry["attempt_id"]:
                return m
        if len(msgs) < 100:
            return None
        after = msgs[-1]["_id"]
    raise RuntimeError("verification window exceeds page limit")


class Delivery:
    def __init__(self, cfg, api, inbox, state):
        self.cfg = cfg
        self.api = api
        self.inbox = inbox
        self.state = state

    # --- reply ---------------------------------------------------------------

    def send(self, msg_id, text, part=None, reply_to="auto", mention_reply=False,
             attachments=None):
        channel, source = split_msg_id(msg_id)
        if self.inbox.get(msg_id) is None:
            return Outcome(FAIL_CLOSED, "unknown-source", msg_id=msg_id)
        k, n = part or (1, 1)
        key = msg_id if part is None else f"{msg_id}#{k}"
        final = part is None or k == n
        ack_keys = [key] + ([msg_id] if final and key != msg_id else [])
        if reply_to == "auto":
            reply_to = source if k == 1 else None
        files = []
        if attachments:
            limits = self.api.media_limits()
            try:
                files = media.inspect(attachments, self.cfg.attach_types,
                                      limits["max_bytes"], limits["max_per_message"])
            except media.MediaError as e:
                return Outcome(NOT_SENT, e.result, **e.info)
        if not text.strip() and not files:
            return Outcome(NOT_SENT, "empty-text")
        if len(text) > MAX_LEN:
            return Outcome(NOT_SENT, "too-long", length=len(text), max=MAX_LEN,
                           hint="split into parts with --part k/n")
        try:
            bot = bot_identity(self.cfg, self.api)
        except Exception as e:
            return Outcome(FAIL_CLOSED, "identity-unavailable", detail=str(e)[:200])

        with self.state.transaction() as sends:
            if self.inbox.is_done(key):
                sends.pop(key, None)
                if final:
                    self.inbox.ack([msg_id])
                return Outcome(OK, "already-done", key=key)
            entry = sends.get(key)
            action = None
            if entry:
                status = entry["status"]
                if status == "retry_wait":
                    if time.time() < entry["retry_at"]:
                        return Outcome(RETRY_WAIT, "retry-deferred", key=key,
                                       retry_at=entry["retry_at"])
                    sends.pop(key)  # a 429 proves nothing was stored
                    entry = None
                elif status == "unacked":
                    action = "ack"
                elif status == "sending" and owner_alive(entry):
                    return Outcome(UNCERTAIN, "held-sending", key=key)
                else:
                    entry["status"] = "uncertain"
                    action = "verify"
            if entry is None:
                entry = {"status": "sending", "key": key, "msg_id": msg_id,
                         "channel": channel, "reply_to": reply_to,
                         "attempt_id": str(uuid.uuid4()), "claimed_at": time.time(),
                         "pid": os.getpid(), "host": socket.gethostname(),
                         "text_sha256": hashlib.sha256(text.encode()).hexdigest()}
                if files:
                    entry["attachments"] = [{k: f[k] for k in ("name", "size", "mime", "sha256")}
                                            for f in files]
                sends[key] = entry
                action = "send"
            entry = dict(entry)

        if action == "ack":
            return self._ack(key, ack_keys, entry.get("message_id"), "acked-later")
        if action == "verify":
            return self._verify(key, ack_keys, entry, bot)

        ids = []
        for f in files:
            try:
                ids.append(self.api.upload_file(f["path"], f["mime"], f["name"]))
            except Exception as e:
                with self.state.transaction() as sends:
                    sends.pop(key, None)
                return Outcome(NOT_SENT, "upload-failed", key=key, file=f["name"],
                               detail=str(e)[:200])
        res = self.api.send_message(channel, text.strip() and text, entry["attempt_id"],
                                    reply_to=reply_to, mention_reply=mention_reply,
                                    attachments=ids)
        if res.kind == "sent":
            outcome = self._mark_sent(key, ack_keys, res.message["_id"], "replied")
            if ids:
                outcome.info["attachments"] = [a.get("_id") for a in
                                               res.message.get("attachments") or []] or ids
            return outcome
        if res.kind == "not_sent":
            with self.state.transaction() as sends:
                sends.pop(key, None)
            return Outcome(NOT_SENT, "send-failed", key=key, detail=res.detail)
        if res.kind == "retry_wait":
            with self.state.transaction() as sends:
                sends[key] = dict(entry, status="retry_wait", retry_at=res.retry_at)
            return Outcome(RETRY_WAIT, "retry-deferred", key=key, retry_at=res.retry_at)
        with self.state.transaction() as sends:
            sends[key] = dict(entry, status="uncertain", last_error=res.detail)
        return self._verify(key, ack_keys, entry, bot)

    def _mark_sent(self, key, ack_keys, message_id, result):
        with self.state.transaction() as sends:
            current = sends.get(key, {})
            sends[key] = dict(current, status="unacked", message_id=message_id)
        return self._ack(key, ack_keys, message_id, result)

    def _ack(self, key, ack_keys, message_id, result):
        try:
            self.inbox.ack(ack_keys, disposition="replied", message_id=message_id)
        except Exception as e:
            return Outcome(UNACKED, "sent-unacked", key=key, message_id=message_id,
                           detail=f"{type(e).__name__}: {e}"[:200])
        with self.state.transaction() as sends:
            sends.pop(key, None)
        return Outcome(OK, result, key=key, message_id=message_id)

    def _verify(self, key, ack_keys, entry, bot):
        try:
            found = find_attempt(self.api, entry, bot)
        except Exception as e:
            return Outcome(UNCERTAIN, "uncertain-held", key=key,
                           detail=f"verification read failed: {e}"[:200])
        if found:
            return self._mark_sent(key, ack_keys, found["_id"], "verified")
        with self.state.transaction() as sends:
            if key in sends:
                sends[key]["verify_attempts"] = sends[key].get("verify_attempts", 0) + 1
                sends[key]["last_verify_at"] = time.time()
        return Outcome(UNCERTAIN, "uncertain-held", key=key,
                       attempt_id=entry["attempt_id"],
                       detail="no message with this attempt's nonce in history yet")

    # --- quiet completion ------------------------------------------------------

    def no_reply(self, msg_id, reason):
        if not reason.strip():
            return Outcome(NOT_SENT, "reason-required")
        if self.inbox.get(msg_id) is None:
            return Outcome(FAIL_CLOSED, "unknown-source", msg_id=msg_id)
        with self.state.transaction() as sends:
            live = {k: e["status"] for k, e in sends.items()
                    if k == msg_id or k.startswith(msg_id + "#")}
            if live:
                statuses = set(live.values())
                code = UNACKED if statuses == {"unacked"} else \
                    RETRY_WAIT if statuses == {"retry_wait"} else UNCERTAIN
                return Outcome(code, "held-by-send-state", sends=live)
            if self.inbox.is_done(msg_id):
                return Outcome(OK, "already-done")
            self.inbox.ack([msg_id], disposition="no-reply", reason=reason)
        return Outcome(OK, "no-reply")

    # --- operator only -----------------------------------------------------------

    def release(self, key):
        """Forget an attempt after a human confirmed in Stoat it was NOT stored."""
        with self.state.transaction() as sends:
            return sends.pop(key, None)

