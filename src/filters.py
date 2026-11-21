"""Which incoming Stoat messages should wake the agent. Pure decision logic.

Bots receive every event in every channel they can see (there is no
mention-only subscription), so the bridge has to decide itself. The rules:

  * system / webhook / own messages                 -> never queued
  * another bot                                     -> only if it @mentions us
                                                       (and is allowlisted when
                                                       an allowlist is set)
  * human in a DirectMessage channel                -> "dm"
  * human @mentioning us anywhere                   -> "mention"
  * human replying to one of our messages           -> "reply"
                                                       (Stoat has no threads;
                                                       a reply is the follow-up)
  * human in a channel listed in STOAT_LISTEN_CHANNELS -> "channel"
                                                       (opt-in: the worker decides
                                                       whether to answer at all)
  * anything else                                   -> ignored
"""


def mentions_user(msg, user_id):
    if not user_id:
        return False
    return user_id in (msg.get("mentions") or []) \
        or f"<@{user_id}>" in (msg.get("content") or "")


def decide(msg, me, channel_type, author_is_bot, reply_to_me, allowed_bots=frozenset(),
           listen=False):
    """Return (action, detail) with action in {"queue", "own", "ignore"}.

    For "queue", detail is the kind (dm | mention | reply | channel); otherwise a
    reason. `listen` is True for channels in STOAT_LISTEN_CHANNELS.
    `author_is_bot` and `reply_to_me` are zero-arg callables so that their
    lookups (which may need the network) only run when the rule needs them.
    """
    if msg.get("system") or not msg.get("author"):
        return "ignore", "system-message"
    if msg.get("webhook"):
        return "ignore", "webhook-message"
    author = msg["author"]
    if author == me:
        return "own", "own-message"
    if channel_type == "SavedMessages":
        return "ignore", "saved-messages"

    mentioned = mentions_user(msg, me)
    if author_is_bot():
        if not mentioned:
            return "ignore", "bot-not-mentioning-us"
        if allowed_bots and author not in allowed_bots:
            return "ignore", "bot-not-allowlisted"
        return "queue", "mention"
    if channel_type == "DirectMessage":
        return "queue", "dm"
    if mentioned:
        return "queue", "mention"
    if msg.get("replies") and reply_to_me():
        return "queue", "reply"
    if listen:
        return "queue", "channel"
    return "ignore", "not-addressed-to-us"
