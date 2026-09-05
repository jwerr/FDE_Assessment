#!/usr/bin/env python3
"""
Customer Operations MCP Server
==============================

A stdio-transport MCP server exposing two tools:

  * get_customer_record  – look up a customer by ``CUST-XXXXX`` id
  * trigger_refund       – issue a refund for a customer

Design rules enforced by this file
----------------------------------
1. **stdout is the wire.**  The stdio transport uses stdout exclusively for
   JSON-RPC frames.  Nothing in this module may ``print()`` to stdout.  All
   diagnostics go through ``logging`` configured to write to **stderr** only.
   As a belt-and-braces guard, ``sys.stdout`` is swapped for a sentinel that
   raises if any stray write happens outside the transport.

2. **Strict validation, standard error codes.**  Tool arguments are validated
   with Pydantic models (``extra="forbid"``, ``strict=True``).  Any validation
   failure is raised as ``McpError`` with JSON-RPC code ``-32602`` (Invalid
   params) so the client receives a proper ``error`` object rather than a
   tool result with ``isError: true``.  Unknown tools map to ``-32601``
   (Method not found).  Unexpected failures map to ``-32603`` (Internal error)
   with the real traceback logged to stderr only – never leaked to the client.
"""

from __future__ import annotations

import io
import json
import logging
import sys
from decimal import Decimal
from typing import Any

import anyio
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
try:  # mcp >= 2.0 renamed the class
    from mcp.shared.exceptions import MCPError as McpError
except ImportError:  # mcp 1.x
    from mcp.shared.exceptions import McpError

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("customer-mcp")


class _ForbiddenStdout(io.TextIOBase):
    """Sentinel that turns any accidental stdout write into a loud failure.

    The stdio transport grabs the *real* stdout fd before this is installed
    (see ``main``), so protocol frames are unaffected. Any ``print()`` left in
    application code will raise here instead of silently corrupting the stream.
    """

    def write(self, s: str) -> int:  # noqa: D401
        raise RuntimeError(
            "Attempted write to stdout outside the JSON-RPC transport. "
            "Use logging (stderr) instead."
        )

    def writable(self) -> bool:
        return True

CUSTOMER_ID_PATTERN = r"^CUST-\d{5}$"


class _StrictModel(BaseModel):
    """Base config: reject unknown fields, no implicit type coercion."""

    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=False)


class GetCustomerRecordInput(_StrictModel):
    customer_id: str = Field(
        ...,
        pattern=CUSTOMER_ID_PATTERN,
        description="Customer identifier in the form CUST-XXXXX (five digits).",
        examples=["CUST-00042"],
    )


class TriggerRefundInput(_StrictModel):
    customer_id: str = Field(
        ...,
        pattern=CUSTOMER_ID_PATTERN,
        description="Customer identifier in the form CUST-XXXXX (five digits).",
        examples=["CUST-00042"],
    )
    amount: float = Field(
        ...,
        gt=0,
        description="Refund amount. Must be a positive, finite number.",
        examples=[49.99],
    )
    reason: str = Field(
        ...,
        min_length=10,
        max_length=500,
        description="Human-readable justification (minimum 10 characters).",
        examples=["Customer returned damaged goods"],
    )

    @field_validator("amount")
    @classmethod
    def _finite_amount(cls, v: float) -> float:
        # ``gt=0`` alone accepts ``inf``; NaN also slips past comparisons.
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("amount must be a finite number")
        # Guard against sub-cent precision abuse (e.g. 0.000001).
        if Decimal(str(v)).as_tuple().exponent < -2:
            raise ValueError("amount may have at most two decimal places")
        return v

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        # min_length counts whitespace; "          " must not pass.
        if len(v.strip()) < 10:
            raise ValueError("reason must contain at least 10 non-whitespace characters")
        return v


_CUSTOMERS: dict[str, dict[str, Any]] = {
    "CUST-00001": {"customer_id": "CUST-00001", "name": "Ada Lovelace", "email": "ada@example.com", "tier": "gold", "balance": 120.50},
    "CUST-00002": {"customer_id": "CUST-00002", "name": "Alan Turing", "email": "alan@example.com", "tier": "silver", "balance": 0.00},
    "CUST-00042": {"customer_id": "CUST-00042", "name": "Grace Hopper", "email": "grace@example.com", "tier": "platinum", "balance": 980.00},
}
_refund_counter = 0


def _invalid_params(exc: ValidationError, tool: str) -> McpError:
    """Translate a Pydantic ValidationError into JSON-RPC -32602."""
    issues = [
        {
            "field": ".".join(str(p) for p in e["loc"]) or "<root>",
            "message": e["msg"],
            "type": e["type"],
        }
        for e in exc.errors(include_url=False, include_input=False)
    ]
    return McpError(
        types.ErrorData(
            code=types.INVALID_PARAMS,
            message=f"Invalid params for tool '{tool}'",
            data={"tool": tool, "issues": issues},
        )
    )


def _method_not_found(tool: str) -> McpError:
    return McpError(
        types.ErrorData(
            code=types.METHOD_NOT_FOUND,
            message=f"Unknown tool: '{tool}'",
            data={"tool": tool, "available": [t.name for t in TOOLS]},
        )
    )


def _internal_error(tool: str) -> McpError:
    
    return McpError(
        types.ErrorData(
            code=types.INTERNAL_ERROR,
            message="Internal error while executing tool",
            data={"tool": tool},
        )
    )


def _validate(model: type[BaseModel], arguments: Any, tool: str) -> BaseModel:
    if not isinstance(arguments, dict):
        raise McpError(
            types.ErrorData(
                code=types.INVALID_PARAMS,
                message=f"Invalid params for tool '{tool}': arguments must be an object",
                data={"tool": tool},
            )
        )
    try:
        return model.model_validate(arguments)
    except ValidationError as exc:
        log.warning("Validation failed for %s: %s", tool, exc.error_count())
        raise _invalid_params(exc, tool) from None


TOOLS: list[types.Tool] = [
    types.Tool(
        name="get_customer_record",
        description="Fetch a customer's profile by id (format CUST-XXXXX).",
        inputSchema=GetCustomerRecordInput.model_json_schema(),
    ),
    types.Tool(
        name="trigger_refund",
        description=(
            "Issue a refund to a customer. Requires a positive amount and a "
            "reason of at least 10 characters."
        ),
        inputSchema=TriggerRefundInput.model_json_schema(),
    ),
]


async def _get_customer_record(args: GetCustomerRecordInput) -> dict[str, Any]:
    record = _CUSTOMERS.get(args.customer_id)
    if record is None:
        # Well-formed id but no such customer: this is a *business* outcome,
        # not a protocol error, so it is returned as structured data.
        return {"found": False, "customer_id": args.customer_id}
    return {"found": True, "record": record}


async def _trigger_refund(args: TriggerRefundInput) -> dict[str, Any]:
    global _refund_counter
    record = _CUSTOMERS.get(args.customer_id)
    if record is None:
        return {"status": "rejected", "reason": "customer_not_found", "customer_id": args.customer_id}
    _refund_counter += 1
    refund_id = f"RF-{_refund_counter:06d}"
    log.info("Refund %s issued: %s %.2f (%s)", refund_id, args.customer_id, args.amount, args.reason)
    return {
        "status": "approved",
        "refund_id": refund_id,
        "customer_id": args.customer_id,
        "amount": round(args.amount, 2),
        "reason": args.reason,
    }

server = Server("customer-ops", version="1.0.0")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return TOOLS


async def handle_call_tool(req: types.CallToolRequest) -> types.ServerResult:
    """tools/call handler.

    Registered directly in ``server.request_handlers`` rather than through
    ``@server.call_tool``: that decorator catches *every* exception and
    converts it into a ``CallToolResult(isError=True)``.  The assessment
    requires validation failures to surface as JSON-RPC ``error`` objects
    with standard codes, so this handler raises ``McpError`` and lets the
    SDK's dispatcher (``_handle_request``) serialise it as an error response.
    """
    name = req.params.name
    arguments: Any = req.params.arguments if req.params.arguments is not None else {}
    log.info("tools/call name=%s", name)

    if name == "get_customer_record":
        parsed = _validate(GetCustomerRecordInput, arguments, name)
        handler = _get_customer_record
    elif name == "trigger_refund":
        parsed = _validate(TriggerRefundInput, arguments, name)
        handler = _trigger_refund
    else:
        raise _method_not_found(name)

    try:
        payload = await handler(parsed)  
    except McpError:
        raise
    except Exception:
        log.exception("Unhandled error in tool %s", name)
        raise _internal_error(name) from None

    return types.ServerResult(
        types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(payload))],
            structuredContent=payload,
            isError=False,
        )
    )


server.request_handlers[types.CallToolRequest] = handle_call_tool


async def main() -> None:
    log.info("Starting customer-ops MCP server on stdio")
    # stdio_server binds to the real stdout *before* we install the guard.
    async with stdio_server() as (read_stream, write_stream):
        sys.stdout = _ForbiddenStdout()
        try:
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )
        finally:
            sys.stdout = sys.__stdout__
    log.info("Server shut down cleanly")


if __name__ == "__main__":
    anyio.run(main)
