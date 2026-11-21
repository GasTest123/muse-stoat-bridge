"""Minimal Stoat (Revolt) REST client on the standard library.

Read calls retry transient failures and honour 429 `retry_after`. The one
write call, `send_message`, never retries: it classifies the outcome so the
caller's durable state machine can decide (see delivery.py).
"""
import http.client
import json
import os
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import media

USER_AGENT = "muse-stoat-bridge/1.0 (+muse)"

# Failures that prove the request never reached the server.
_NOT_SENT_REASONS = (ConnectionRefusedError, socket.gaierror, ssl.SSLError)


class ApiError(Exception):
    def __init__(self, message, status=None, body=None):
        super().__init__(message)
        self.status = status
        self.body = body


class SendResult:
    """kind: sent | not_sent | uncertain | retry_wait"""

    def __init__(self, kind, message=None, retry_at=None, detail=""):
        self.kind = kind
        self.message = message
        self.retry_at = retry_at
        self.detail = detail

    def __repr__(self):
        return f"SendResult({self.kind}, detail={self.detail!r})"


def _parse(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return raw.decode("utf-8", "replace")


def _retry_after_secs(data, headers, default):
    ms = None
    if isinstance(data, dict) and isinstance(data.get("retry_after"), (int, float)):
        ms = data["retry_after"]
    elif headers is not None and headers.get("X-RateLimit-Reset-After"):
        try:
            ms = float(headers.get("X-RateLimit-Reset-After"))
        except ValueError:
            ms = None
    return default if ms is None else max(ms / 1000.0, 0.5)


class StoatAPI:
    def __init__(self, cfg, timeout=20):
        self.cfg = cfg
        self.timeout = timeout
        handlers = []
        if cfg.proxy_url:
            handlers.append(urllib.request.ProxyHandler(
                {"http": cfg.proxy_url, "https": cfg.proxy_url}))
        ctx = ssl.create_default_context(cafile=cfg.ca_bundle) if cfg.ca_bundle \
            else ssl.create_default_context()
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
        self._opener = urllib.request.build_opener(*handlers)
        self._root = None

    def _root_cached(self):
        if self._root is None:
            self._root = self.root()
        return self._root

    def _url(self, path, params=None):
        url = self.cfg.api_url + path
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean)
        return url

    def _raw(self, method, path, body=None, headers=None, params=None, timeout=None):
        """Return (status, headers, parsed body). Transport errors propagate."""
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self._url(path, params), data=data, method=method)
        req.add_header("User-Agent", USER_AGENT)
        if self.cfg.token:
            req.add_header("X-Bot-Token", self.cfg.token)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with self._opener.open(req, timeout=timeout or self.timeout) as resp:
                return resp.status, resp.headers, _parse(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, e.headers, _parse(e.read())

    def get_json(self, path, params=None, attempts=3):
        last = None
        for attempt in range(attempts):
            try:
                status, headers, data = self._raw("GET", path, params=params)
            except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
                last = ApiError(f"GET {path}: {type(e).__name__}: {e}")
                time.sleep(min(2 ** attempt, 5))
                continue
            if status == 429:
                last = ApiError(f"GET {path}: rate limited", status, data)
                time.sleep(min(_retry_after_secs(data, headers, 5.0), 30))
                continue
            if 200 <= status < 300:
                return data
            if status >= 500:
                last = ApiError(f"GET {path} -> {status}", status, data)
                time.sleep(min(2 ** attempt, 5))
                continue
            raise ApiError(f"GET {path} -> {status}: {data}", status, data)
        raise last

    # --- reads -------------------------------------------------------------

    def root(self):
        return self.get_json("/")

    def autumn_url(self):
        """File server base URL: STOAT_AUTUMN_URL, else features.autumn.url."""
        if self.cfg.autumn_url:
            return self.cfg.autumn_url
        autumn = (self._root_cached().get("features") or {}).get("autumn") or {}
        if not autumn.get("enabled") or not autumn.get("url"):
            raise ApiError("this Stoat instance has no file server (features.autumn)")
        return autumn["url"].rstrip("/")

    def media_limits(self):
        """{max_bytes, max_per_message} for attachments, from the server when it says."""
        try:
            limits = (self._root_cached().get("features") or {}).get("limits") or {}
        except ApiError:
            limits = {}
        default = limits.get("default") or {}
        size = (default.get("file_upload_size_limits") or {}).get("attachments")
        count = default.get("message_attachments")
        return {"max_bytes": int(size or media.DEFAULT_MAX_BYTES),
                "max_per_message": int(count or media.DEFAULT_MAX_PER_MESSAGE)}

    def me(self):
        return self.get_json("/users/@me")

    def get_user(self, user_id):
        return self.get_json(f"/users/{user_id}")

    def get_channel(self, channel_id):
        return self.get_json(f"/channels/{channel_id}")

    def get_message(self, channel_id, message_id):
        return self.get_json(f"/channels/{channel_id}/messages/{message_id}")

    def fetch_messages(self, channel_id, limit=100, before=None, after=None,
                       sort=None, nearby=None, include_users=True):
        """Return (messages, users_by_id). Order follows `sort`."""
        params = {"limit": limit, "before": before, "after": after,
                  "sort": sort, "nearby": nearby,
                  "include_users": "true" if include_users else None}
        data = self.get_json(f"/channels/{channel_id}/messages", params=params)
        if isinstance(data, dict):
            msgs = data.get("messages") or []
            users = {u["_id"]: u for u in data.get("users") or [] if "_id" in u}
        else:
            msgs, users = data or [], {}
        return msgs, users

    # --- uploads -------------------------------------------------------------

    def upload_file(self, path, mime, name=None, attempts=3, timeout=300):
        """Upload one file to Autumn's `attachments` bucket; return its id.

        Not idempotent, and that is fine: an id no message ever references is
        just an orphan on the file server, invisible to everyone. So uploads
        retry freely, while the message POST that uses the ids does not.
        """
        url = self.autumn_url() + "/attachments"
        name = "".join(c if c.isprintable() and c not in '"\\' else "_"
                       for c in (name or os.path.basename(path))) or "file"
        boundary = uuid.uuid4().hex
        with open(path, "rb") as f:
            payload = f.read()
        body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
                f"filename=\"{name}\"\r\nContent-Type: {mime}\r\n\r\n").encode() \
            + payload + f"\r\n--{boundary}--\r\n".encode()
        last = None
        for attempt in range(attempts):
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("User-Agent", USER_AGENT)
            req.add_header("X-Bot-Token", self.cfg.token)
            req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
            try:
                with self._opener.open(req, timeout=timeout) as resp:
                    status, headers, data = resp.status, resp.headers, _parse(resp.read())
            except urllib.error.HTTPError as e:
                status, headers, data = e.code, e.headers, _parse(e.read())
            except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
                last = ApiError(f"upload {name}: {type(e).__name__}: {e}")
                time.sleep(min(2 ** attempt, 5))
                continue
            if 200 <= status < 300 and isinstance(data, dict) and data.get("id"):
                return data["id"]
            if status == 429:
                last = ApiError(f"upload {name}: rate limited", status, data)
                time.sleep(min(_retry_after_secs(data, headers, 5.0), 30))
                continue
            if status >= 500 or 200 <= status < 300:
                last = ApiError(f"upload {name} -> {status}: {data}", status, data)
                time.sleep(min(2 ** attempt, 5))
                continue
            kind = data.get("type") if isinstance(data, dict) else data
            raise ApiError(f"upload {name} -> {status} {kind}", status, data)
        raise last

    # --- the one write ------------------------------------------------------

    def send_message(self, channel_id, content, attempt_id, reply_to=None,
                     mention_reply=False, attachments=None, timeout=30):
        body = {}
        if content:
            body["content"] = content
        if attachments:
            body["attachments"] = list(attachments)
        if reply_to:
            body["replies"] = [{"id": reply_to, "mention": bool(mention_reply),
                                "fail_if_not_exists": False}]
        # The attempt id goes in the Idempotency-Key header only. The server
        # stores it as the message's `nonce`, which is what verification reads
        # back. Sending the same value in body `nonce` too is rejected as a
        # duplicate by the server's nonce cache.
        headers = {"Idempotency-Key": attempt_id}
        try:
            status, resp_headers, data = self._raw(
                "POST", f"/channels/{channel_id}/messages", body, headers, timeout=timeout)
        except urllib.error.URLError as e:
            if isinstance(e.reason, _NOT_SENT_REASONS):
                return SendResult("not_sent", detail=f"connect: {type(e.reason).__name__}")
            return SendResult("uncertain", detail=f"transport: {type(e.reason).__name__}")
        except Exception as e:  # reset / timeout / truncated response after sending
            return SendResult("uncertain", detail=f"transport: {type(e).__name__}")

        if 200 <= status < 300:
            if isinstance(data, dict) and data.get("_id"):
                return SendResult("sent", message=data)
            return SendResult("uncertain", detail=f"{status} without message body")
        if status == 429:
            return SendResult("retry_wait",
                              retry_at=time.time() + _retry_after_secs(data, resp_headers, 60.0),
                              detail="rate limited")
        kind = data.get("type") if isinstance(data, dict) else None
        if status == 409:
            # The key was already consumed: an earlier attempt may or may not
            # have been stored. Only history verification can tell.
            return SendResult("uncertain", detail=f"409 {kind or 'conflict'}")
        if 400 <= status < 500 and status != 408:
            return SendResult("not_sent", detail=f"{status} {kind or data}")
        return SendResult("uncertain", detail=f"{status} {kind or ''}".strip())
