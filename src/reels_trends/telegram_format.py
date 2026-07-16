from html import escape
import re

TELEGRAM_MAX_MESSAGE_LENGTH = 4096

_TRAILING_ENTITY_RE = re.compile(r"&[#a-zA-Z0-9]*$")


def utf16_len(s: str) -> int:
    """Telegram enforces message-length limits in UTF-16 code units, not codepoints."""
    return sum(2 if ord(ch) > 0xFFFF else 1 for ch in s)


def truncate_utf16(s: str, max_len: int) -> str:
    if max_len <= 0:
        return ""
    result = []
    total = 0
    for ch in s:
        width = 2 if ord(ch) > 0xFFFF else 1
        if total + width > max_len:
            break
        result.append(ch)
        total += width
    return "".join(result)


def escape_and_truncate_caption(raw_caption: str, budget: int) -> str:
    caption = escape(raw_caption.strip())
    if utf16_len(caption) <= budget:
        return caption
    ellipsis = "…"
    truncated = truncate_utf16(caption, max(budget - utf16_len(ellipsis), 0))
    truncated = _TRAILING_ENTITY_RE.sub("", truncated)
    return truncated + ellipsis
