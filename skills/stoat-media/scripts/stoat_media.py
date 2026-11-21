#!/usr/bin/env python3
"""Make generated images / videos ready for Stoat, without the bridge's code.

  stoat_media.py prepare FILE... [--max-bytes N] [--out-dir DIR]
  stoat_media.py demo image|video --out FILE [--label TEXT]

prepare prints one JSON line per input and exits 0 when every file is ready:

  {"input": ..., "output": <path to attach>, "kind": "image"|"video",
   "mime": ..., "size": ..., "changed": bool, "note": ...}

What it fixes (needs ffmpeg/ffprobe; on macOS `sips` covers images too):
  - formats Stoat does not preview (bmp, tiff, ...) -> png / mp4
  - videos that are not mp4/webm -> mp4 (H.264 + AAC, faststart)
  - files over the size limit -> jpeg with lower quality/size, or a video
    re-encoded at the bitrate that fits

A file that cannot be made ready gets {"output": null, "error": ...} and the
exit code is 1. Exit 2 means a usage error or a missing tool.

demo makes a labelled placeholder (pure-Python PNG; ffmpeg test pattern for
video) for checking the delivery path when no real generator is available.
Never present a demo file as generated content.

Standard library only; Python 3.9+.
"""
import argparse
import json
import os
import shutil
import struct
import subprocess
import sys
import zlib

DEFAULT_MAX_BYTES = 20_000_000
WEB_VIDEO = {"video/mp4", "video/webm"}
PREVIEW_IMAGE = {"image/png", "image/jpeg", "image/gif", "image/webp"}


def sniff(path):
    """(kind, mime) from magic bytes, or None. Mirrors the bridge's check."""
    with open(path, "rb") as f:
        head = f.read(64)
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
        if brand in (b"avif", b"avis"):
            return "image", "image/avif"
        if brand in (b"heic", b"heix", b"mif1"):
            return "image", "image/heic"
        if brand == b"qt  ":
            return "video", "video/quicktime"
        return "video", "video/mp4"
    if head.startswith(b"\x1a\x45\xdf\xa3"):
        return "video", "video/webm" if b"webm" in head else "video/x-matroska"
    return None


def have(tool):
    return shutil.which(tool) is not None


def run(cmd):
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{os.path.basename(cmd[0])} failed: "
                           + (proc.stderr.strip().splitlines() or ["?"])[-1][:300])
    return proc.stdout


def probe(path):
    """ffprobe facts: (codec_type of the first stream, frames>1?, duration secs)."""
    out = run(["ffprobe", "-v", "error", "-show_entries",
               "stream=codec_type,nb_frames:format=duration", "-of", "json", path])
    data = json.loads(out)
    streams = data.get("streams") or []
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    try:
        duration = float((data.get("format") or {}).get("duration") or 0)
    except ValueError:
        duration = 0.0
    frames = int(video.get("nb_frames") or 0) if video and str(
        video.get("nb_frames", "")).isdigit() else 0
    still = video is not None and (frames == 1 or duration == 0)
    return (video is not None), still, duration


def out_path(src, out_dir, ext):
    base = os.path.splitext(os.path.basename(src))[0]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{base}.stoat{ext}")
    n = 1
    while os.path.exists(path):
        n += 1
        path = os.path.join(out_dir, f"{base}.stoat{n}{ext}")
    return path


def to_png(src, out_dir):
    dst = out_path(src, out_dir, ".png")
    if have("ffmpeg"):
        run(["ffmpeg", "-v", "error", "-y", "-i", src, "-frames:v", "1", dst])
    elif have("sips"):
        run(["sips", "-s", "format", "png", src, "--out", dst])
    else:
        raise RuntimeError("converting this image needs ffmpeg (or sips on macOS)")
    return dst


def shrink_image(src, out_dir, max_bytes):
    """Re-encode as JPEG, lowering quality, then size, until it fits."""
    for edge in (4096, 2560, 1920, 1280):
        for quality in (88, 78, 65):
            dst = out_path(src, out_dir, ".jpg")
            if have("ffmpeg"):
                q = {88: 3, 78: 6, 65: 10}[quality]
                run(["ffmpeg", "-v", "error", "-y", "-i", src, "-frames:v", "1", "-vf",
                     f"scale='min({edge},iw)':'min({edge},ih)':force_original_aspect_ratio=decrease",
                     "-q:v", str(q), dst])
            elif have("sips"):
                run(["sips", "-s", "format", "jpeg", "-s", "formatOptions", str(quality),
                     "-Z", str(edge), src, "--out", dst])
            else:
                raise RuntimeError("shrinking images needs ffmpeg (or sips on macOS)")
            if os.path.getsize(dst) <= max_bytes:
                return dst
            os.remove(dst)
    raise RuntimeError("still over the size limit at 1280px / low quality")


def encode_video(src, out_dir, max_bytes, duration):
    """H.264/AAC mp4; when a size cap applies, aim the bitrate at it."""
    if not (have("ffmpeg") and have("ffprobe")):
        raise RuntimeError("converting or shrinking video needs ffmpeg and ffprobe")
    attempts = [(None, 1920)]
    if duration > 0:
        total = max_bytes * 8 * 0.9 / duration  # bits per second, 10% headroom
        video_bps = int(total - 96_000)
        if video_bps < 120_000:
            raise RuntimeError(f"{duration:.0f}s is too long to fit in {max_bytes} bytes; "
                               "cut it shorter")
        attempts = [(video_bps, 1920 if video_bps > 2_500_000 else 1280),
                    (int(video_bps * 0.75), 960), (int(video_bps * 0.55), 640)]
    for bitrate, edge in attempts:
        dst = out_path(src, out_dir, ".mp4")
        cmd = ["ffmpeg", "-v", "error", "-y", "-i", src, "-map", "0:v:0", "-map", "0:a:0?",
               "-vf", f"scale='min({edge},iw)':-2", "-c:v", "libx264", "-preset", "veryfast",
               "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart"]
        if bitrate:
            cmd += ["-b:v", str(bitrate), "-maxrate", str(bitrate), "-bufsize", str(bitrate * 2)]
        else:
            cmd += ["-crf", "23"]
        run(cmd + [dst])
        if os.path.getsize(dst) <= max_bytes:
            return dst
        os.remove(dst)
    raise RuntimeError("could not get the video under the size limit")


def prepare_one(src, max_bytes, out_dir):
    if not os.path.isfile(src):
        raise RuntimeError("no such file")
    found = sniff(src)
    path = src
    if found is None or found[1] not in PREVIEW_IMAGE | WEB_VIDEO:
        if not have("ffprobe"):
            raise RuntimeError(f"format {found[1] if found else 'unknown'} is not previewable "
                               "in Stoat and ffprobe is missing to convert it")
        is_visual, still, duration = probe(src)
        if not is_visual:
            raise RuntimeError("not an image or a video")
        path = to_png(src, out_dir) if still else encode_video(src, out_dir, max_bytes, duration)
        found = sniff(path)
    if os.path.getsize(path) > max_bytes:
        if found[0] == "image":
            path = shrink_image(path, out_dir, max_bytes)
        else:
            _, _, duration = probe(path)
            path = encode_video(path, out_dir, max_bytes, duration)
        found = sniff(path)
    return {"input": src, "output": os.path.abspath(path), "kind": found[0], "mime": found[1],
            "size": os.path.getsize(path), "changed": path != src}


def cmd_prepare(args):
    ok = True
    for src in args.files:
        out_dir = args.out_dir or os.path.join(os.path.dirname(os.path.abspath(src)),
                                               "stoat-ready")
        try:
            rec = prepare_one(src, args.max_bytes, out_dir)
        except Exception as e:  # report per file, keep going
            ok = False
            rec = {"input": src, "output": None, "error": str(e)[:400]}
        print(json.dumps(rec, ensure_ascii=False), flush=True)
    return 0 if ok else 1


def png_bytes(width, height, pixel):
    rows = b"".join(b"\x00" + b"".join(bytes(pixel(x, y)) for x in range(width))
                    for y in range(height))

    def chunk(tag, data):
        return struct.pack(">I", len(data)) + tag + data + \
            struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height,
                                                             8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows, 9)) + chunk(b"IEND", b""))


def cmd_demo(args):
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    seed = zlib.crc32(args.label.encode()) & 0xFF
    if args.kind == "image":
        size = 256

        def pixel(x, y):
            return ((x + seed) % 256, (y * 2 + seed) % 256, (x ^ y) % 256)
        with open(args.out, "wb") as f:
            f.write(png_bytes(size, size, pixel))
    else:
        if not have("ffmpeg"):
            print(json.dumps({"error": "demo video needs ffmpeg"}))
            return 2
        hue = seed * 360 // 256
        run(["ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i",
             f"testsrc2=size=640x360:rate=24:duration={args.seconds}", "-f", "lavfi", "-i",
             f"sine=frequency=440:duration={args.seconds}", "-vf", f"hue=h={hue}",
             "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p", "-c:a", "aac",
             "-shortest", "-movflags", "+faststart", args.out])
    kind, mime = sniff(args.out)
    print(json.dumps({"output": os.path.abspath(args.out), "kind": kind, "mime": mime,
                      "size": os.path.getsize(args.out), "demo": True, "label": args.label}))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare", help="make files attachable (format + size)")
    p.add_argument("files", nargs="+")
    p.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES,
                   help="per-file limit; take it from `stoatctl media-limits`")
    p.add_argument("--out-dir", help="where converted files go (default: <dir>/stoat-ready)")
    p.set_defaults(fn=cmd_prepare)
    d = sub.add_parser("demo", help="placeholder media for testing the delivery path")
    d.add_argument("kind", choices=["image", "video"])
    d.add_argument("--out", required=True)
    d.add_argument("--label", default="demo")
    d.add_argument("--seconds", type=int, default=3)
    d.set_defaults(fn=cmd_demo)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
