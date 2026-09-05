#!/usr/bin/env python3
"""
End-to-end tests for server.py over a real stdio transport.

Two layers of testing:

1. **Raw wire test** (``test_stdout_is_pure_jsonrpc``): spawns the server as a
   subprocess, feeds it hand-written JSON-RPC lines, and asserts that *every*
   line on stdout is a valid JSON-RPC message.  This is the STDIO-isolation
   check the assessment asks for.

2. **SDK client tests**: uses the official ``mcp`` client over stdio to exercise
   validation and error mapping (-32602 / -32601) plus the happy paths.

Run:  python -m pytest test_server.py -v
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
try:  # mcp >= 2.0 renamed the class
    from mcp.shared.exceptions import MCPError as McpError
except ImportError:  # mcp 1.x
    from mcp.shared.exceptions import McpError
from mcp.types import INTERNAL_ERROR, INVALID_PARAMS, METHOD_NOT_FOUND

SERVER = Path(__file__).with_name("server.py")
PARAMS = StdioServerParameters(command=sys.executable, args=[str(SERVER)])


# --------------------------------------------------------------------------- #
# 1. Raw transport test: stdout must contain ONLY JSON-RPC frames
# --------------------------------------------------------------------------- #
def test_stdout_is_pure_jsonrpc():
    frames = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize",
         "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": {"name": "raw-test", "version": "0"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
         "params": {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00042"}}},
        {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
         "params": {"name": "get_customer_record", "arguments": {"customer_id": "bogus"}}},
        {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
         "params": {"name": "trigger_refund",
                    "arguments": {"customer_id": "CUST-00042", "amount": 12.5, "reason": "Damaged on arrival"}}},
    ]
    proc = subprocess.Popen(
        [sys.executable, str(SERVER)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    stdout_lines: list[str] = []
    try:
        for frame in frames:
            proc.stdin.write(json.dumps(frame) + "\n")
            proc.stdin.flush()
            if "id" in frame:  # requests get exactly one response line; notifications get none
                stdout_lines.append(proc.stdout.readline())
        _, stderr = proc.communicate(timeout=10)  # closes stdin, drains stderr
    finally:
        proc.kill()

    assert stdout_lines, "server produced no output on stdout"
    for ln in stdout_lines:
        msg = json.loads(ln)  # raises if any non-JSON garbage leaked to stdout
        assert msg.get("jsonrpc") == "2.0", f"non-JSON-RPC line on stdout: {ln!r}"
        assert "id" in msg or "method" in msg

    # Logs must have gone to stderr, not stdout.
    assert "Starting customer-ops" in stderr
    assert not any("Starting customer-ops" in ln for ln in stdout_lines)

    by_id = {m["id"]: m for m in map(json.loads, stdout_lines) if "id" in m}
    assert "result" in by_id[2] and len(by_id[2]["result"]["tools"]) == 2
    assert "result" in by_id[3]
    assert by_id[4]["error"]["code"] == INVALID_PARAMS
    assert "result" in by_id[5]


# --------------------------------------------------------------------------- #
# 2. SDK client tests
# --------------------------------------------------------------------------- #
@pytest.fixture
async def session():
    async with stdio_client(PARAMS) as (read, write):
        async with ClientSession(read, write) as s:
            await s.initialize()
            yield s


async def _call(session: ClientSession, name: str, args: dict):
    return await session.call_tool(name, args)


async def _expect_error(session: ClientSession, name: str, args, code: int) -> dict:
    with pytest.raises(McpError) as ei:
        await session.call_tool(name, args)
    err = ei.value.error
    assert err.code == code, f"expected {code}, got {err.code}: {err.message}"
    return err.model_dump()


@pytest.mark.anyio
async def test_list_tools(session):
    tools = (await session.list_tools()).tools
    names = {t.name for t in tools}
    assert names == {"get_customer_record", "trigger_refund"}
    refund = next(t for t in tools if t.name == "trigger_refund")
    assert set(refund.inputSchema["required"]) == {"customer_id", "amount", "reason"}
    assert refund.inputSchema["properties"]["reason"]["minLength"] == 10
    assert refund.inputSchema["properties"]["amount"]["exclusiveMinimum"] == 0


@pytest.mark.anyio
async def test_get_customer_happy_path(session):
    res = await _call(session, "get_customer_record", {"customer_id": "CUST-00042"})
    assert not res.isError
    assert res.structuredContent["found"] is True
    assert res.structuredContent["record"]["name"] == "Grace Hopper"


@pytest.mark.anyio
async def test_get_customer_not_found_is_not_a_protocol_error(session):
    res = await _call(session, "get_customer_record", {"customer_id": "CUST-99999"})
    assert not res.isError
    assert res.structuredContent == {"found": False, "customer_id": "CUST-99999"}


@pytest.mark.anyio
@pytest.mark.parametrize("bad_id", [
    "cust-00042",     # lowercase prefix
    "CUST-0042",      # 4 digits
    "CUST-000420",    # 6 digits
    "CUST-ABCDE",     # letters
    "CUST00042",      # missing hyphen
    " CUST-00042",    # leading whitespace
    "CUST-00042\n",   # trailing newline
    "",               # empty
])
async def test_get_customer_bad_id_format(session, bad_id):
    err = await _expect_error(session, "get_customer_record", {"customer_id": bad_id}, INVALID_PARAMS)
    assert err["data"]["issues"][0]["field"] == "customer_id"


@pytest.mark.anyio
async def test_get_customer_wrong_type_and_missing_and_extra(session):
    await _expect_error(session, "get_customer_record", {"customer_id": 42}, INVALID_PARAMS)
    await _expect_error(session, "get_customer_record", {}, INVALID_PARAMS)
    err = await _expect_error(
        session, "get_customer_record", {"customer_id": "CUST-00042", "evil": 1}, INVALID_PARAMS
    )
    assert any(i["type"] == "extra_forbidden" for i in err["data"]["issues"])


@pytest.mark.anyio
async def test_refund_happy_path(session):
    res = await _call(session, "trigger_refund",
                      {"customer_id": "CUST-00001", "amount": 25.00, "reason": "Duplicate charge on invoice"})
    assert not res.isError
    sc = res.structuredContent
    assert sc["status"] == "approved" and sc["refund_id"].startswith("RF-") and sc["amount"] == 25.0


@pytest.mark.anyio
@pytest.mark.parametrize("args, field", [
    ({"customer_id": "CUST-00001", "amount": 0, "reason": "Zero amount refund test"}, "amount"),
    ({"customer_id": "CUST-00001", "amount": -5, "reason": "Negative amount refund"}, "amount"),
    ({"customer_id": "CUST-00001", "amount": "10.00", "reason": "String amount not allowed"}, "amount"),
    ({"customer_id": "CUST-00001", "amount": 1e400, "reason": "Infinity is not finite"}, "amount"),
    ({"customer_id": "CUST-00001", "amount": 0.001, "reason": "Sub-cent precision"}, "amount"),
    ({"customer_id": "CUST-00001", "amount": 10, "reason": "short"}, "reason"),
    ({"customer_id": "CUST-00001", "amount": 10, "reason": "            "}, "reason"),
    ({"customer_id": "CUST-00001", "amount": 10, "reason": None}, "reason"),
    ({"customer_id": "CUST-00001", "amount": 10}, "reason"),
    ({"customer_id": "CUST-1", "amount": 10, "reason": "Bad customer id here"}, "customer_id"),
])
async def test_refund_validation_errors(session, args, field):
    err = await _expect_error(session, "trigger_refund", args, INVALID_PARAMS)
    assert any(i["field"] == field for i in err["data"]["issues"]), err


@pytest.mark.anyio
async def test_refund_reports_all_issues_at_once(session):
    err = await _expect_error(session, "trigger_refund",
                              {"customer_id": "nope", "amount": -1, "reason": "x"}, INVALID_PARAMS)
    fields = {i["field"] for i in err["data"]["issues"]}
    assert fields == {"customer_id", "amount", "reason"}


@pytest.mark.anyio
async def test_unknown_tool_is_method_not_found(session):
    err = await _expect_error(session, "delete_everything", {}, METHOD_NOT_FOUND)
    assert "available" in err["data"]


@pytest.mark.anyio
async def test_error_codes_are_standard():
    assert INVALID_PARAMS == -32602
    assert METHOD_NOT_FOUND == -32601
    assert INTERNAL_ERROR == -32603
