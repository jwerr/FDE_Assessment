"""
Streaming PII redactor
======================

The problem: an LLM's response arrives as a stream of small deltas, and a
sensitive value can be split across them::

    "Contact jo" | "hn.doe@exa" | "mple.com or" | " 4111 1111 1" | "111 1111."

No single delta matches a regex, and buffering the whole response until the
end would destroy time-to-first-token.

The solution: a *bounded holdback* buffer.  After each delta we look at the
**tail** of what we have and ask "could this tail be the beginning of a
sensitive pattern?"  If the tail is a run of email-safe characters or a run
of digits/separators it might be, so we hold it; everything before it is
provably safe to emit *now*.  When the next delta arrives it either extends
the run (still held) or terminates it (then the run is scanned, redacted if
it matches, and released).

Guarantees
----------
* **Correctness:**  the concatenated output is identical to running the
  redaction over the complete text in one go, regardless of how the text is
  chunked.  ``test_redactor.py`` proves this with randomised chunkings.
* **Latency:**  a delta is delayed only until the *next delimiter* arrives
  (typically one token), never until the end of the stream.
* **Memory:**  the buffer never exceeds ``MAX_HOLD + len(largest delta)``
  characters, independent of response length.

Patterns
--------
* Email      – RFC-ish local@domain.tld
* SSN        – ``AAA-GG-SSSS`` with the SSA's structural rules (area ≠ 000,
               666, 9xx; group ≠ 00; serial ≠ 0000)
* Card (PAN) – 13-19 digits with optional space/dash separators, **Luhn
               validated** so that order numbers and timestamps that happen
               to be 16 digits long are left alone.

Adding a pattern = one ``Pattern`` entry plus (if it uses a new character
class) a candidate regex telling the holdback logic what a *prefix* of it
looks like.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Pattern as RePattern

REDACTED = "[REDACTED]"

# --------------------------------------------------------------------------- #
# Full-match patterns (applied to text that is known to be complete)
# --------------------------------------------------------------------------- #
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

_SSN = re.compile(
    r"(?<!\d)"                          # not preceded by a digit (see note below)
    r"(?!000|666|9\d\d)\d{3}"           # area
    r"([- ]?)"                          # separator (captured to enforce consistency)
    r"(?!00)\d{2}"                      # group
    r"\1"                               # same separator
    r"(?!0000)\d{4}"                    # serial
    r"(?!\d)"
)
# NOTE on context: every pattern's lookarounds only inspect *digits*.  The
# streaming holdback guarantees a digit run is never split, so a pattern that
# only looks at digits around itself behaves identically whether it sees the
# whole text or just the run.  A lookbehind on, say, a dash would break that
# guarantee (the dash may already have been emitted).

_CARD = re.compile(r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)")


def _luhn_ok(s: str) -> bool:
    digits = [int(c) for c in s if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total, parity = 0, len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


@dataclass(frozen=True)
class Pattern:
    name: str
    regex: RePattern[str]
    validate: Callable[[str], bool] | None = None  # extra check on the match text

    def sub(self, text: str, replacement: str) -> str:
        if self.validate is None:
            return self.regex.sub(replacement, text)
        return self.regex.sub(
            lambda m: replacement if self.validate(m.group(0)) else m.group(0), text
        )


DEFAULT_PATTERNS: tuple[Pattern, ...] = (
    Pattern("credit_card", _CARD, _luhn_ok),   # before SSN: longer digit runs first
    Pattern("ssn", _SSN),
    Pattern("email", _EMAIL),
)


def redact(text: str, patterns: tuple[Pattern, ...] = DEFAULT_PATTERNS, replacement: str = REDACTED) -> str:
    """Whole-text redaction. The streaming redactor is defined to match this."""
    for p in patterns:
        text = p.sub(text, replacement)
    return text


# --------------------------------------------------------------------------- #
# Holdback candidates: what does an *incomplete* sensitive value look like?
# --------------------------------------------------------------------------- #
# A trailing run of characters that may legally appear inside an email.
# (Digits, dots, dashes are included, so this also covers a number run
# followed by no delimiter yet.)
_EMAIL_TAIL = re.compile(r"[A-Za-z0-9._%+@-]+\Z")
# A trailing run that starts with a digit and continues with digits and the
# separators SSNs / cards allow.  Includes the trailing separator itself
# ("4111 " could continue with more digits).
_NUMBER_TAIL = re.compile(r"\d[\d -]*\Z")

# Upper bound on holdback.  Longest realistic email local part is 64 chars;
# a 19-digit card with separators is 37.  Anything longer than this without
# a delimiter is not PII we recognise (base64, URLs, hashes) and is released.
MAX_HOLD = 96


def holdback_length(buf: str, max_hold: int = MAX_HOLD) -> int:
    """How many trailing chars of *buf* might be the start of a pattern.

    Two candidate classes are checked and the longer wins.  One subtlety:
    the email class contains digits but not spaces, while the number class
    contains spaces but not letters.  So for ``"4111 1111 1111 1111."`` the
    email tail is only ``"1111."`` — cutting there would split the card.
    When the email-class tail *starts with a digit*, we therefore extend the
    hold backwards through any number run that immediately precedes it.
    """
    n = len(buf)
    start = n

    m = _NUMBER_TAIL.search(buf)
    if m:
        start = m.start()

    m = _EMAIL_TAIL.search(buf)
    if m:
        e = m.start()
        if e < n and buf[e].isdigit():
            prev = _NUMBER_TAIL.search(buf, 0, e)
            if prev:
                e = prev.start()
        start = min(start, e)

    return min(n - start, max_hold)


# --------------------------------------------------------------------------- #
# The streaming redactor
# --------------------------------------------------------------------------- #
@dataclass
class StreamRedactor:
    """Feed deltas in, get safe text out.  One instance per response stream."""

    patterns: tuple[Pattern, ...] = DEFAULT_PATTERNS
    replacement: str = REDACTED
    max_hold: int = MAX_HOLD

    _buf: str = field(default="", init=False, repr=False)
    # instrumentation
    max_buffer_seen: int = field(default=0, init=False)
    redactions: int = field(default=0, init=False)

    def feed(self, delta: str) -> str:
        """Absorb *delta*, return whatever is now provably safe to emit."""
        if not delta:
            return ""
        self._buf += delta
        if len(self._buf) > self.max_buffer_seen:
            self.max_buffer_seen = len(self._buf)

        hold = holdback_length(self._buf, self.max_hold)
        cut = len(self._buf) - hold
        if cut <= 0:
            return ""
        safe, self._buf = self._buf[:cut], self._buf[cut:]
        return self._scan(safe)

    def flush(self) -> str:
        """End of stream: everything left is complete, scan and release it."""
        safe, self._buf = self._buf, ""
        return self._scan(safe)

    @property
    def pending(self) -> int:
        return len(self._buf)

    def _scan(self, text: str) -> str:
        out = redact(text, self.patterns, self.replacement)
        if out != text:
            self.redactions += out.count(self.replacement) - text.count(self.replacement)
        return out
