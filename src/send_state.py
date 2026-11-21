"""Durable send-attempt state: `send_state.json`.

One entry per in-flight send key, status one of:
  sending     claimed by a live process, POST may be in flight
  uncertain   POST outcome unknown; only history verification may resolve it
  unacked     Stoat accepted the message; only the inbox tombstone is missing
  retry_wait  Stoat said 429; nothing sent; do not retry before retry_at
An entry is removed once the inbox tombstone is durable. Completed keys are
never stored here: the tombstone in inbox.jsonl is the completion record.

Corruption is fail-closed: an unreadable file is moved aside, a
`.quarantined` marker is written, and every later open refuses to run until
an operator inspects and removes the marker.
"""
import copy
import fcntl
import json
import os
import socket
import time
from contextlib import contextmanager

VERSION = 1
STATUSES = {"sending", "uncertain", "unacked", "retry_wait"}


class StateCorruptError(Exception):
    pass


def _fsync_dir(path):
    fd = os.open(os.path.dirname(os.path.abspath(path)), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def owner_alive(entry):
    """True if the process that claimed `entry` may still be sending."""
    if entry.get("host") != socket.gethostname():
        return True  # cannot tell; assume alive
    try:
        os.kill(int(entry.get("pid", -1)), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (TypeError, ValueError):
        return False


class SendState:
    def __init__(self, path, lock_path=None):
        self.path = path
        self.lock_path = lock_path or path + ".lock"
        self.marker = path + ".quarantined"

    def _validate(self, data):
        if not isinstance(data, dict) or data.get("version") != VERSION:
            raise ValueError("bad version")
        sends = data.get("sends")
        if not isinstance(sends, dict):
            raise ValueError("sends is not an object")
        for key, entry in sends.items():
            if not isinstance(entry, dict) or entry.get("status") not in STATUSES:
                raise ValueError(f"bad entry {key!r}")

    def _load(self):
        if os.path.exists(self.marker):
            raise StateCorruptError(f"send state quarantined, see {self.marker}")
        if not os.path.exists(self.path):
            return {"version": VERSION, "sends": {}}
        try:
            with open(self.path) as f:
                data = json.load(f)
            self._validate(data)
            return data
        except (ValueError, OSError) as e:
            evidence = f"{self.path}.corrupt.{int(time.time() * 1000)}"
            os.replace(self.path, evidence)
            with open(self.marker, "w") as f:
                f.write(f"{time.ctime()}: {type(e).__name__}: {e}\nevidence: {evidence}\n"
                        "Sends are refused until a human restores a verified copy "
                        "and deletes this marker.\n")
            raise StateCorruptError(f"send state corrupt ({e}); quarantined to {evidence}")

    def _save(self, data):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        _fsync_dir(self.path)

    @contextmanager
    def transaction(self):
        """Exclusive read-modify-write of the `sends` dict.

        Lock order is always send_state -> inbox; the inbox never takes this
        lock, so nesting inbox calls inside a transaction is safe."""
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        with open(self.lock_path, "a+") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                data = self._load()
                before = json.dumps(data, sort_keys=True)
                yield data["sends"]
                if json.dumps(data, sort_keys=True) != before:
                    self._save(data)
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

    def snapshot(self):
        with self.transaction() as sends:
            return copy.deepcopy(sends)
