"""ULID helpers.

Stoat message/channel/user ids are ULIDs: a 48-bit millisecond timestamp
followed by 80 random bits, Crockford base32, 26 chars. Equal-length ULIDs
sort lexicographically by time, which the bridge relies on for cursors and
for synthesising `after=` bounds from a wall-clock time.
"""
import os
import time

_ENC = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_DEC = {c: i for i, c in enumerate(_ENC)}


def _encode(value, length):
    out = []
    for _ in range(length):
        out.append(_ENC[value & 31])
        value >>= 5
    return "".join(reversed(out))


def time_ms(ulid):
    value = 0
    for c in ulid[:10].upper():
        value = value * 32 + _DEC[c]
    return value


def from_ms(ms, randomness=0):
    """Smallest ULID for `ms` by default: a valid lower bound for `after=`."""
    return _encode(int(ms), 10) + _encode(int(randomness), 16)


def new(ms=None):
    ms = time.time() * 1000 if ms is None else ms
    return from_ms(ms, int.from_bytes(os.urandom(10), "big"))


class Monotonic:
    """Strictly increasing ULIDs, even within one millisecond."""

    def __init__(self):
        self._ms = -1
        self._rand = 0

    def next(self, ms=None):
        ms = int(time.time() * 1000) if ms is None else int(ms)
        if ms <= self._ms:
            ms, rand = self._ms, self._rand + 1
        else:
            rand = int.from_bytes(os.urandom(10), "big") >> 1
        self._ms, self._rand = ms, rand
        return from_ms(ms, rand)
