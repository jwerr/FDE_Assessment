# Customer Operations MCP Server

A simple MCP server built with the official Python `mcp` SDK.

The server communicates through **stdio** and provides two tools:

| Tool | Description |
|---|---|
| `get_customer_record` | Retrieves a customer record using a customer ID |
| `trigger_refund` | Creates a refund request for a customer |

## Tool Inputs

### `get_customer_record`

Requires:

- `customer_id`
- Format: `CUST-12345`
- The ID must start with `CUST-` followed by exactly 5 digits.

Example:

```json
{
  "customer_id": "CUST-12345"
}
```

### `trigger_refund`

Requires:

- `customer_id` — same `CUST-12345` format
- `amount` — must be greater than 0 and have no more than 2 decimal places
- `reason` — must contain at least 10 non-whitespace characters

Example:

```json
{
  "customer_id": "CUST-12345",
  "amount": 25.50,
  "reason": "Order arrived damaged"
}
```

## Run the Server

Install the required packages:

```bash
pip install -r requirements.txt
```

Start the MCP server:

```bash
python server.py
```

You can also test the server using the MCP Inspector:

```bash
npx @modelcontextprotocol/inspector python server.py
```

## Example Request

You can send MCP JSON-RPC messages directly through stdin:

```bash
printf '%s\n' \
'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"cli","version":"0"}}}' \
'{"jsonrpc":"2.0","method":"notifications/initialized"}' \
'{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_customer_record","arguments":{"customer_id":"bogus"}}}' \
| python server.py 2>/dev/null
```

Because `bogus` is not a valid customer ID, the server returns a validation error similar to:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "error": {
    "code": -32602,
    "message": "Invalid params for tool 'get_customer_record'"
  }
}
```

## Claude Desktop / Cursor Configuration

Add the server to your MCP configuration:

```json
{
  "mcpServers": {
    "customer-ops": {
      "command": "python",
      "args": ["/absolute/path/to/server.py"]
    }
  }
}
```

Replace `/absolute/path/to/server.py` with the actual location of the file.

## Running Tests

Run the test suite with:

```bash
python -m pytest test_server.py -v
```

The project includes **27 tests** covering both normal requests and invalid inputs.

The tests check things such as:

- Valid customer lookups
- Valid refund requests
- Missing required fields
- Incorrect data types
- Invalid customer IDs
- Extra fields
- Zero or negative refund amounts
- Invalid decimal amounts
- `NaN` or infinite amounts
- Empty or very short refund reasons
- Unknown tool names
- Correct JSON-RPC responses
- Logs staying out of stdout

## Error Handling

The server uses standard JSON-RPC error codes:

| Situation | Error Code |
|---|---:|
| Invalid tool arguments | `-32602` |
| Arguments are not a JSON object | `-32602` |
| Unknown tool | `-32601` |
| Unexpected server error | `-32603` |
| Invalid JSON | `-32700` |
| Invalid JSON-RPC request | `-32600` |

If a valid customer ID does not exist, the server does **not** treat it as a protocol error.

Instead, it returns a normal response such as:

```json
{
  "found": false
}
```

This allows the calling AI or application to decide what to do next.

## Validation

The server uses **Pydantic** for strict input validation.

It prevents common invalid inputs, including:

- Extra fields
- String values where numbers are expected
- Invalid customer ID formats
- Leading or trailing whitespace in customer IDs
- Refund amounts with more than 2 decimal places
- Infinite or `NaN` refund amounts
- Refund reasons containing only spaces

For example:

```json
{
  "amount": "10"
}
```

is rejected because the amount must be a JSON number, not a string.

The server can also return multiple validation problems in one response instead of making the client fix them one at a time.

## STDIO Safety

MCP uses stdout to communicate with the client, so normal log messages must not accidentally appear there.

This project sends all logs to:

```text
stderr
```

while MCP JSON-RPC messages are sent through:

```text
stdout
```

This keeps the communication channel clean and prevents log messages from breaking the MCP protocol.

## Project Files

```text
server.py
    Main MCP server

test_server.py
    Automated pytest tests

requirements.txt
    Python dependencies

pytest.ini
    Pytest configuration
```

## Summary

This project demonstrates a small MCP server with:

- Two MCP tools
- Strict Pydantic validation
- Clean JSON-RPC error handling
- Safe stdio communication
- Structured validation errors
- Automated testing
- Claude Desktop and Cursor compatibility