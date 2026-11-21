"""Durable inbox queue: `inbox.jsonl`, append-only.

Each line is either a message record (has `msg_id`, no `type`) or a
completion tombstone `{"type": "ack", "msg_id": ..., "disposition": ...}`.
Nothing is rewritten in place except by `compact()`, which replaces the file
atomically. A separate lock file guards every read and write.

`msg_id` is `<channel_id>:<message_id>`. Tombstones may also carry part keys
(`<msg_id>#k`) written by multi-part sends; those never hide a record.
"""
import fcntl
import json
import os
import time
from contextlib import contextmanager

DEFAULT_RETENTION = 7 * 86400


def _fsync_dir(path):
    fd = os.open(os.path.dirname(os.path.abspath(path)) or ".", os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Inbox:
    def __init__(self, path, lock_path=None):
        self.path = path
        self.lock_path = lock_path or path + ".lock"

    @contextmanager
    def _locked(self, exclusive):
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        with open(self.lock_path, "a+") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

    def _entries(self):
        if not os.path.exists(self.path):
            return []
        out = []
        with open(self.path, "rb") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict) and obj.get("msg_id"):
                    out.append(obj)
        return out

    @staticmethod
    def _is_tomb(obj):
        return obj.get("type") == "ack"

    def _repair_tail(self):
        """Move a half-written last line (crash mid-append) aside before appending."""
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            return
        with open(self.path, "r+b") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) == b"\n":
                return
            f.seek(0)
            data = f.read()
            cut = data.rfind(b"\n") + 1
            with open(f"{self.path}.corrupt.{int(time.time() * 1000)}", "wb") as ev:
                ev.write(data[cut:])
            f.truncate(cut)
            f.flush()
            os.fsync(f.fileno())

    def _append(self, objs):
        created = not os.path.exists(self.path)
        with open(self.path, "a") as f:
            for obj in objs:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        if created:
            _fsync_dir(self.path)

    # --- writes --------------------------------------------------------------

    def append(self, record):
        """Persist a message record. Returns False if it was already seen
        (as a record or a tombstone)."""
        with self._locked(True):
            self._repair_tail()
            seen = {e["msg_id"] for e in self._entries()}
            if record["msg_id"] in seen:
                return False
            self._append([record])
            return True

    def ack(self, keys, disposition="replied", **fields):
        """Write completion tombstones for keys not already completed."""
        with self._locked(True):
            self._repair_tail()
            done = {e["msg_id"] for e in self._entries() if self._is_tomb(e)}
            now = time.time()
            new = [dict(fields, type="ack", msg_id=k, at=now, disposition=disposition)
                   for k in keys if k not in done]
            if new:
                self._append(new)
            return len(new)

    def compact(self, retention=DEFAULT_RETENTION, now=None):
        """Drop completed records and expired tombstones. Returns (kept, dropped)."""
        now = time.time() if now is None else now
        with self._locked(True):
            entries = self._entries()
            done = {e["msg_id"] for e in entries if self._is_tomb(e)}
            keep = [e for e in entries
                    if (self._is_tomb(e) and e.get("at", now) > now - retention)
                    or (not self._is_tomb(e) and e["msg_id"] not in done)]
            tmp = self.path + ".compact.tmp"
            with open(tmp, "w") as f:
                for e in keep:
                    f.write(json.dumps(e, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            _fsync_dir(self.path)
            return len(keep), len(entries) - len(keep)

    # --- reads ---------------------------------------------------------------

    def pending(self):
        """Message records without a tombstone, oldest first."""
        with self._locked(False):
            entries = self._entries()
        done = {e["msg_id"] for e in entries if self._is_tomb(e)}
        return [e for e in entries if not self._is_tomb(e) and e["msg_id"] not in done]

    def get(self, msg_id):
        with self._locked(False):
            for e in self._entries():
                if not self._is_tomb(e) and e["msg_id"] == msg_id:
                    return e
        return None

    def tombstone(self, key):
        with self._locked(False):
            for e in self._entries():
                if self._is_tomb(e) and e["msg_id"] == key:
                    return e
        return None

    def is_done(self, key):
        return self.tombstone(key) is not None
