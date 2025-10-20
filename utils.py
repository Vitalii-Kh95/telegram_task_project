import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from telethon.types import Channel, Chat, Message

from settings import CHAT_IDENTIFIER
import settings


async def _is_group(chat_identifier: str) -> bool:
    entity = await settings.client.get_entity(chat_identifier)
    return isinstance(entity, Chat) or (
        isinstance(entity, Channel) and bool(getattr(entity, "megagroup", False))
    )


async def _is_supergroup(chat_identifier: str) -> bool:
    entity = await settings.client.get_entity(chat_identifier)
    return isinstance(entity, Channel) and bool(getattr(entity, "megagroup", False))


def _normalize_chat_identifier(s: str):
    # если случайно передали ссылку https://t.me/Python — вытащим username
    if s.startswith("https://t.me/"):
        return s.split("https://t.me/")[-1].strip("/")
    return s


def _normalize_date(dt: datetime) -> datetime:
    if dt.tzinfo:
        # Приводим к UTC и делаем offset-naive
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _extract_sender_id(msg: Message):
    """Return a normalized sender ID as string (if available)."""
    if not getattr(msg, "from_id", None):
        return None
    raw = msg.from_id
    return str(getattr(raw, "user_id", raw))


def _build_output(per_day, msgs_by_id):
    output = {}

    for day, threads in per_day.items():
        day_list = []
        for (ttype, tid), info in threads.items():
            topic_text = info["topic_candidate"]
            if not topic_text:
                # try to find root message text from local cache if thread is reply
                if ttype == "reply":
                    root_msg = msgs_by_id.get(tid)
                    topic_text = (root_msg.message or "").strip() if root_msg else None
            topic_text = topic_text or "[No text]"
            day_list.append(
                {
                    "topic": topic_text.replace("\n", " ")[:200],
                    "messages": info["messages"],
                    "users": len(
                        {p for p in info["participants"] if p not in (None, "None")}
                    ),
                }
            )
        # sort by messages desc
        day_list.sort(key=lambda x: x["messages"], reverse=True)
        output[day] = day_list

    return output


async def collect_last_7_days_messages(chat_identifier):
    chat_identifier = _normalize_chat_identifier(str(chat_identifier))

    is_group = await _is_group(chat_identifier)
    if not is_group:
        raise Exception("Not a group or supergroup")

    cutoff = datetime.utcnow() - timedelta(days=7)
    messages = []

    async for msg in settings.client.iter_messages(chat_identifier, reverse=False):
        if not isinstance(msg, Message) or not msg.date:
            continue

        msg.date = _normalize_date(msg.date)

        if msg.date < cutoff:
            break
        messages.append(msg)

    return messages


# --- place helper above your aggregation functions ---
def _resolve_canonical_thread(
    msg, msgs_by_id, topic_attrs=("message_thread_id", "topic_id", "message_thread")
):
    """
    Return (thread_type, thread_id, root_msg_obj_or_None)

    Behavior:
      - If the message or any ancestor has a topic id (topic_attrs), return ("topic", topic_id).
      - Else if message (or ancestor) is a reply, climb the reply chain using msgs_by_id
        until you reach the highest ancestor present in msgs_by_id (or stop when no parent).
        Return ("reply", topmost_id).
      - Else return ("root", msg.id).
    """
    # 1) Check message itself for topic id
    for a in topic_attrs:
        v = getattr(msg, a, None)
        if v:
            return "topic", int(v), msgs_by_id.get(int(getattr(msg, a)))

    # 2) If it's a reply, climb upward while parent exists in local cache
    reply_to = getattr(msg, "reply_to_msg_id", None)
    if reply_to:
        cur_id = int(reply_to)
        parent = msgs_by_id.get(cur_id)
        # climb while parent exists and itself is a reply to something also in msgs_by_id
        while parent:
            # if parent has a topic id, prefer topic grouping
            for a in topic_attrs:
                pv = getattr(parent, a, None)
                if pv:
                    return "topic", int(pv), parent
            parent_reply = getattr(parent, "reply_to_msg_id", None)
            if parent_reply and int(parent_reply) in msgs_by_id:
                cur_id = int(parent_reply)
                parent = msgs_by_id.get(cur_id)
                continue
            # stop climbing: either parent has no reply_to or its parent isn't in cache
            break
        # cur_id is the topmost ancestor we can see (may be outside cache if parent was None)
        return "reply", cur_id, msgs_by_id.get(cur_id)

    # 3) fallback: root (this message is not a reply and has no topic id)
    return "root", int(msg.id), msgs_by_id.get(int(msg.id))


# ---------- For plain groups (no forum topics) ----------
def aggregate_group_messages(messages):
    """
    messages: list[telethon.types.Message] (already filtered to last 7 days)
    Return: { "YYYY-MM-DD": [ {"topic": str, "messages": int, "users": int}, ... ] }
    Behavior:
      - Threads are reply-chains: root message id -> all replies to it.
      - We resolve canonical root (climb replies) so replies-to-replies fall under same root.
    """
    msgs_by_id = {int(m.id): m for m in messages if isinstance(m, Message)}

    per_day = defaultdict(
        lambda: defaultdict(
            lambda: {"messages": 0, "participants": set(), "topic_candidate": None}
        )
    )

    for msg in messages:
        day = msg.date.date().isoformat()

        ttype, thread_id, root_msg = _resolve_canonical_thread(msg, msgs_by_id)
        thread_key = (ttype, int(thread_id))

        rec = per_day[day][thread_key]
        rec["messages"] += 1

        sender = _extract_sender_id(msg)
        if sender:
            rec["participants"].add(sender)

        # Prefer the root message text as the topic candidate for reply threads
        if ttype == "reply":
            if not rec["topic_candidate"] and root_msg:
                rec["topic_candidate"] = (root_msg.message or "").strip() or None
        elif ttype == "root":
            # If this thread is a root (this msg is the root), prefer this text
            if not rec["topic_candidate"]:
                rec["topic_candidate"] = (msg.message or "").strip() or None
        elif ttype == "topic":
            # topic threads: prefer the first non-empty textual message we see
            if not rec["topic_candidate"]:
                txt = (msg.message or "").strip()
                if txt:
                    rec["topic_candidate"] = txt

    # Build output: day -> list of threads with topic/messages/users
    output = _build_output(per_day, msgs_by_id)
    return output


# ---------- For supergroups (forum-enabled or with topics) ----------
def aggregate_supergroup_messages(messages):
    """
    messages: list[telethon.types.Message] (already filtered to last 7 days)
    Return: { "YYYY-MM-DD": [ {"topic": str, "messages": int, "users": int}, ... ] }
    Behavior:
      - Prefer grouping by topic/thread id (message_thread_id / topic_id variants).
      - If missing, fall back to canonical reply root (climb replies).
      - Topic label: prefer root text for reply threads; otherwise first non-empty text in topic/root.
    """
    msgs_by_id = {int(m.id): m for m in messages if isinstance(m, Message)}

    per_day = defaultdict(
        lambda: defaultdict(
            lambda: {"messages": 0, "participants": set(), "topic_candidate": None}
        )
    )

    topic_attrs = ("message_thread_id", "topic_id", "message_thread")

    for msg in messages:
        day = msg.date.date().isoformat()

        # use the canonical resolver which prefers topic ids anywhere in the chain
        ttype, thread_id, root_msg = _resolve_canonical_thread(
            msg, msgs_by_id, topic_attrs
        )
        thread_key = (ttype, int(thread_id))

        rec = per_day[day][thread_key]
        rec["messages"] += 1

        sender = _extract_sender_id(msg)
        if sender:
            rec["participants"].add(sender)

        # Decide topic_candidate:
        if ttype == "topic":
            # Prefer first non-empty message text seen in the topic
            if not rec["topic_candidate"]:
                txt = (msg.message or "").strip()
                if txt:
                    rec["topic_candidate"] = txt
        elif ttype == "reply":
            # Prefer the root message text (if available in cache)
            if not rec["topic_candidate"] and root_msg:
                rec["topic_candidate"] = (root_msg.message or "").strip() or None
        else:  # root
            if not rec["topic_candidate"]:
                rec["topic_candidate"] = (msg.message or "").strip() or None

    # Build output
    output = _build_output(per_day, msgs_by_id)
    return output


async def aggregate_messages(messages):
    is_supergroup = await _is_supergroup(CHAT_IDENTIFIER)

    if is_supergroup:
        result = aggregate_supergroup_messages(messages)
    else:
        result = aggregate_group_messages(messages)

    return result


def filter_discussed_threads(d):
    filtered_result = {}
    for day, threads in d.items():
        filtered_threads = [
            t
            for t in threads
            if t["messages"] > 1  # This is the minimal amount of replies!
        ]
        if filtered_threads:
            filtered_result[day] = filtered_threads

    return filtered_result


def format_for_json_output(per_day, timezone="Asia/Tashkent"):
    """Reformat analysis results into the required JSON structure."""
    output = {"timezone": timezone, "days": []}

    for day, threads in sorted(per_day.items()):
        day_entry = {"date": day, "threads": []}

        for t in threads:
            topic_name = (t.get("topic") or "[No text]").replace("\n", " ")[:200]
            day_entry["threads"].append(
                {
                    "topic": topic_name,
                    "messages": int(t.get("messages", 0)),
                    "users": int(t.get("users", 0)),
                }
            )

        output["days"].append(day_entry)

    return output


async def save_results_to_json(data, filename="messages_7days.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
