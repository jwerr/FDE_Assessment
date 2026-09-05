"""Unit tests for the streaming redactor. Run: python -m pytest test_redactor.py -v"""

from __future__ import annotations

import random
import re

import pytest

from redactor import MAX_HOLD, REDACTED, StreamRedactor, holdback_length, redact


# --------------------------------------------------------------------------- #
# Whole-text pattern behaviour
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text, expected", [
    ("mail john.doe@example.com now", "mail [REDACTED] now"),
    ("x first.last+tag@sub.domain.org y", "x [REDACTED] y"),
    ("card 4111 1111 1111 1111 ok", "card [REDACTED] ok"),
    ("card 4111-1111-1111-1111 ok", "card [REDACTED] ok"),
    ("card 4111111111111111 ok", "card [REDACTED] ok"),
    ("amex 378282246310005 ok", "amex [REDACTED] ok"),
    ("ssn 123-45-6789 ok", "ssn [REDACTED] ok"),
    ("ssn 123 45 6789 ok", "ssn [REDACTED] ok"),
    ("ssn 123456789 ok", "ssn [REDACTED] ok"),
    ("all: a@b.co, 5500-0000-0000-0004, 078-05-1120.", "all: [REDACTED], [REDACTED], [REDACTED]."),
])
def test_redacts(text, expected):
    assert redact(text) == expected


@pytest.mark.parametrize("text", [
    "order 1234 5678 9012 3456 shipped",       # 16 digits, fails Luhn → not a card
    "timestamp 1725000000123 ok",             # 13 digits, fails Luhn
    "ssn-like 000-12-3456 and 666-11-2222 and 900-11-2222",   # invalid SSN areas
    "ssn-like 123-00-4567 and 123-45-0000",   # invalid group / serial
    "mixed sep 123-45 6789",                  # inconsistent separators
    "not an email: user@localhost or @handle or a@b",
    "phone (555) 123-4567 and version 1.2.3",
    "the year 2024 and pi 3.14159",
])
def test_leaves_alone(text):
    assert redact(text) == text


def test_card_inside_longer_digit_run_is_not_redacted():
    # 20 digits: not a PAN, and we don't carve a PAN out of the middle of it.
    assert redact("id 41111111111111111111 x") == "id 41111111111111111111 x"


# --------------------------------------------------------------------------- #
# THE invariant: streaming output == whole-text output for any chunking
# --------------------------------------------------------------------------- #
def _stream(text: str, sizes: list[int]) -> tuple[str, StreamRedactor]:
    r = StreamRedactor()
    out, i, k = "", 0, 0
    while i < len(text):
        n = sizes[k % len(sizes)]
        out += r.feed(text[i:i + n])
        i += n
        k += 1
    out += r.flush()
    return out, r


SAMPLES = [
    "Contact john.doe@example.com or 4111 1111 1111 1111. SSN 123-45-6789. Order #1234567890123456 (not a card).",
    "Emails: a@b.co, first.last+tag@sub.domain.org; cards 5500-0000-0000-0004 and 378282246310005; ssn 078 05 1120.",
    "No PII here at all, just a normal paragraph of text with numbers like 2024 and 3.14159 and words.",
    "edge@x.io4111111111111111 and 4111111111111111@x.io, 4111 1111 1111 1111abc, 4111-1111-1111-1111@, 123-45-6789. done",
    "(555) 123-4567 and 4111 1111 1111 1111, ok.\nNew line 123-45-6789\n\n378282246310005",
    "trailing card 4111 1111 1111 1111",
    "trailing email a@b.co",
    "trailing ssn 123-45-6789",
]


@pytest.mark.parametrize("text", SAMPLES)
@pytest.mark.parametrize("sizes", [[1], [2], [3], [5], [7], [1, 40], [13, 1, 1, 8], [100]])
def test_streaming_matches_whole_text(text, sizes):
    out, _ = _stream(text, sizes)
    assert out == redact(text)


def test_every_single_split_point():
    """Split each sample at *every* index into two chunks."""
    for text in SAMPLES:
        expected = redact(text)
        for i in range(len(text) + 1):
            r = StreamRedactor()
            out = r.feed(text[:i]) + r.feed(text[i:]) + r.flush()
            assert out == expected, f"split at {i}: {text[:i]!r} | {text[i:]!r}"


def test_fuzz_random_chunkings():
    """3000 adversarial texts (PII glued to junk) × random chunkings."""
    rng = random.Random(2024)
    pii = ["a.b@c.de", "4111 1111 1111 1111", "123-45-6789", "378282246310005",
           "5500-0000-0000-0004", "x@y.zz", "first.last+tag@sub.domain.org", "078 05 1120", "078-05-1120"]
    junk = ["hello", " ", ".", ",", "-", "12", "3456", "@", "\n", "abc123", "  ", "!", "9",
            " ", " ", "(", ")", ": ", "\t"]
    run_re = re.compile(r"[A-Za-z0-9._%+@ -]+")
    for _ in range(3000):
        text = "".join(rng.choice(pii if rng.random() < 0.3 else junk) for _ in range(rng.randint(3, 30)))
        if any(len(run) > MAX_HOLD for run in run_re.findall(text)):
            continue  # delimiter-free runs beyond the cap are explicitly best-effort
        expected = redact(text)
        for _ in range(5):
            sizes = [rng.choice([1, 1, 2, 3, 5, 8, 13, 40]) for _ in range(8)]
            out, _ = _stream(text, sizes)
            assert out == expected, (text, sizes)


# --------------------------------------------------------------------------- #
# Latency: text is released as soon as a delimiter proves it safe
# --------------------------------------------------------------------------- #
def test_releases_on_delimiter_not_at_end():
    r = StreamRedactor()
    assert r.feed("Hello") == ""            # could be the start of Hello@x.com
    assert r.feed(", ") == "Hello, "        # comma+space terminate the run → released
    assert r.feed("world") == ""
    assert r.feed("!") == "world!"          # "!" is not a pattern char, whole tail released
    assert r.feed(" my card is 4111 1111") == " my card is "
    assert r.feed(" 1111 1111") == ""
    assert r.feed(".") == ""                # "." is an email char; card still pending
    assert r.feed(" ") == "[REDACTED]. "    # space after "." closes both runs
    assert r.flush() == ""


def test_plain_prose_latency_is_one_token():
    """For ordinary prose, each word is released when the following space arrives."""
    r = StreamRedactor()
    words = "the quick brown fox jumps over the lazy dog".split()
    released = []
    for w in words:
        released.append(r.feed(w + " "))
    assert released == [w + " " for w in words]


# --------------------------------------------------------------------------- #
# Memory: buffer is bounded regardless of stream length
# --------------------------------------------------------------------------- #
def test_buffer_bounded_on_long_stream():
    r = StreamRedactor()
    text = ("lorem ipsum dolor sit amet 4111 1111 1111 1111 consectetur a@b.co " * 5000)  # ~330 KB
    for i in range(0, len(text), 7):
        r.feed(text[i:i + 7])
    r.flush()
    assert r.max_buffer_seen <= MAX_HOLD + 7
    assert r.redactions == 10000


def test_delimiter_free_run_is_capped_and_released():
    r = StreamRedactor()
    out = r.feed("A" * 500)                  # 500 chars, no delimiter
    assert len(out) == 500 - MAX_HOLD        # released all but the cap
    assert r.pending == MAX_HOLD
    assert r.max_buffer_seen == 500


def test_holdback_length_examples():
    assert holdback_length("hello world ") == 0
    assert holdback_length("hello world") == len("world")
    assert holdback_length("call 4111 1111 ") == len("4111 1111 ")
    assert holdback_length("x 4111 1111 1111 1111.") == len("4111 1111 1111 1111.")   # digit-led email tail extends back
    assert holdback_length("x 4111 1111 1111 1111 abc") == len("abc")
    assert holdback_length("line\n") == 0


def test_redaction_counter_and_custom_replacement():
    r = StreamRedactor(replacement="<pii>")
    assert r.feed("a@b.co and 123-45-6789 ") == "<pii> and "   # "6789 " could continue with digits
    assert r.flush() == "<pii> "
    assert r.redactions == 2
