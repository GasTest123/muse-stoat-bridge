"""Local media files the worker wants to attach: type sniffing and limits.

The type is judged by magic bytes, never by the file name, so a prompt that
talks the worker into "attaching" `.env`, a token file or source code fails
here: none of those start with an image or video signature.
"""
import hashlib
import os

# Stoat's defaults (GET / -> features.limits.default); the live values are
# read from the server when it is reachable.
DEFAULT_MAX_BYTES = 20_000_000
DEFAULT_MAX_PER_MESSAGE = 5

_FTYP_IMAGE = {b"avif": "image/avif", b"avis": "image/avif", b"heic": "image/heic",
               b"heix": "image/heic", b"mif1": "image/heif"}
_FTYP_MOV = {b"qt  ": "video/quicktime"}


def sniff(head):
    """(kind, mime) from the first bytes of a file, or None."""
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image", "image/png"
    if head.startswith(b"\xff\xd8\xff"):
        return "image", "image/jpeg"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image", "image/gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image", "image/webp"
    if head[:4] == b"RIFF" and head[8:12] == b"AVI ":
        return "video", "video/x-msvideo"
    if head[4:8] == b"ftyp":
        brand = head[8:12]
        if brand in _FTYP_IMAGE:
            return "image", _FTYP_IMAGE[brand]
        if brand in _FTYP_MOV:
            return "video", _FTYP_MOV[brand]
        return "video", "video/mp4"
    if head.startswith(b"\x1a\x45\xdf\xa3"):
        return "video", "video/webm" if b"webm" in head[:64] else "video/x-matroska"
    return None


class MediaError(Exception):
    def __init__(self, result, **info):
        super().__init__(result)
        self.result = result
        self.info = info


def inspect(paths, allowed_kinds, max_bytes=DEFAULT_MAX_BYTES,
            max_count=DEFAULT_MAX_PER_MESSAGE):
    """Validate files for one message; return [{path, name, size, kind, mime, sha256}]."""
    if len(paths) > max_count:
        raise MediaError("too-many-attachments", count=len(paths), max=max_count,
                         hint="send the rest as further parts (--part k/n)")
    out = []
    for raw in paths:
        path = os.path.abspath(os.path.expanduser(raw))
        if not os.path.isfile(path):
            raise MediaError("attachment-missing", path=raw)
        size = os.path.getsize(path)
        if size == 0:
            raise MediaError("attachment-empty", path=raw)
        if size > max_bytes:
            raise MediaError("attachment-too-large", path=raw, size=size, max=max_bytes,
                             hint="compress it first (skills/stoat-media: stoat_media.py prepare)")
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            head = f.read(64)
            digest.update(head)
            for chunk in iter(lambda: f.read(1 << 20), b""):
                digest.update(chunk)
        found = sniff(head)
        if found is None or found[0] not in allowed_kinds:
            raise MediaError("attachment-type-refused", path=raw,
                             detected=found[1] if found else "unknown",
                             allowed=sorted(allowed_kinds))
        out.append({"path": path, "name": os.path.basename(path), "size": size,
                    "kind": found[0], "mime": found[1], "sha256": digest.hexdigest()})
    return out
