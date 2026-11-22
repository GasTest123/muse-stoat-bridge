"""Shared configuration: `.env` loading and runtime file locations.

Precedence: explicit overrides > process environment > `.env` file > defaults.
Every component (bridge, stoatctl, hook) builds its Config the same way, so
they agree on the data directory as long as they see the same env.
"""
import os

# The project root (this file lives in <root>/src): .env, data/, routes.json
# and hook/ are resolved from there.
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_env_file(path):
    values = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return values


def _csv(raw):
    return {x.strip() for x in (raw or "").split(",") if x.strip()}


class Config:
    def __init__(self, overrides=None):
        env_file = ((overrides or {}).get("STOAT_ENV_FILE") or os.environ.get("STOAT_ENV_FILE")
                    or os.path.join(BASE, ".env"))
        merged = {**_load_env_file(env_file), **os.environ, **(overrides or {})}
        get = merged.get
        self.api_url = (get("STOAT_API_URL") or "https://api.stoat.chat").rstrip("/")
        # Normally discovered from the API root's `ws` field.
        self.ws_url = get("STOAT_WS_URL") or None
        self.token = get("STOAT_BOT_TOKEN") or ""
        self.bot_user_id = get("STOAT_BOT_USER_ID") or None
        self.ca_bundle = get("STOAT_CA_BUNDLE") or None
        self.proxy_url = get("STOAT_PROXY_URL") or None
        # Empty = any bot that @mentions us may wake the agent (collaboration
        # default); non-empty = only these bot user ids.
        self.allowed_bots = _csv(get("STOAT_ALLOWED_BOTS"))
        self.ignore_channels = _csv(get("STOAT_IGNORE_CHANNELS"))
        # Where every human message is queued, @mention or not: channel ids,
        # `server:<server id>` or `*` (see Bridge._listening).
        self.listen_channels = _csv(get("STOAT_LISTEN_CHANNELS"))
        # How the worker treats those un-addressed messages (kind "channel"):
        # all = always answer (like an @mention); judge = only when needed.
        self.listen_reply = (get("STOAT_LISTEN_REPLY") or "all").strip().lower()
        if self.listen_reply not in ("all", "judge"):
            raise ValueError(f"STOAT_LISTEN_REPLY must be all or judge, "
                             f"not {self.listen_reply!r}")
        # Autumn (file server) base URL; normally discovered from the API
        # root's features.autumn.url.
        self.autumn_url = (get("STOAT_AUTUMN_URL") or "").rstrip("/") or None
        # Which kinds of local file `stoatctl send --attach` may upload,
        # judged by the file's magic bytes (see media.py): image, video.
        self.attach_types = _csv(get("STOAT_ATTACH_TYPES") or "image,video")
        unknown = self.attach_types - {"image", "video"}
        if unknown:
            raise ValueError(f"STOAT_ATTACH_TYPES: unknown kind(s) {sorted(unknown)}; "
                             "use image and/or video")
        # Let every hook run start the bridge when it is not running (the
        # installed setup in Muse: no service file needed).
        self.autostart = (get("STOAT_AUTOSTART") or "0").strip().lower() in ("1", "true", "yes")
        self.backfill_max_age = int(get("STOAT_BACKFILL_MAX_AGE_SECS") or 3 * 86400)
        self.data_dir = os.path.abspath(get("STOAT_DATA_DIR") or os.path.join(BASE, "data"))
        # Channel -> route (one Muse hook each) mapping, see routes.py.
        self.routes_file = os.path.abspath(get("STOAT_ROUTES_FILE")
                                           or os.path.join(BASE, "routes.json"))
        # Where `stoatctl routes hooks --write` puts per-route hook scripts.
        self.hooks_dir = os.path.abspath(get("STOAT_HOOKS_DIR")
                                         or os.path.join(BASE, "hook", "routes"))
        self.route = get("STOAT_ROUTE") or None

    def path(self, name):
        os.makedirs(self.data_dir, exist_ok=True)
        return os.path.join(self.data_dir, name)
