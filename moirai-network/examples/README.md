# Moirai Network Examples

## AWSet Node Example

A simple network node using Add-Wins Set CRDT without requiring Arachne codegen.

**Note**: This example uses a newtype wrapper pattern to implement `QueryableLog` and enable the `/api/state` endpoint. Arachne-generated CRDTs get `QueryableLog` automatically.

### Quick Start

#### 1. Start the server

```bash
cd /path/to/moirai
REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 cargo run -p moirai-network --example awset_node
```

You should see:
```
[a] TCP transport listening on port 9001
[a] HTTP API listening on http://0.0.0.0:8081
[a] AWSet node running. Submit ops to http://localhost:8081/api/op
```

#### 2. Add three elements

In a separate terminal, add three items to the set:

```bash
# Add "apple"
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '{"Add":"apple"}'

# Add "banana"
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '{"Add":"banana"}'

# Add "cherry"
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '{"Add":"cherry"}'
```

Each command should respond with:
```json
{"success":true,"message":"Applied and broadcasted"}
```

#### 3. Remove one element

Remove "banana" from the set:

```bash
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '{"Remove":"banana"}'
```

#### 4. View the current state

Check the computed CRDT state (after applying all operations):

```bash
curl http://localhost:8081/api/state
```

You should see:
```json
["apple", "cherry"]
```

Note: `banana` was removed, so only `apple` and `cherry` remain.

#### 5. View all operations (optional)

You can also see the full operation log:

```bash
curl http://localhost:8081/api/operations
```

```json
{
  "operations": [
    {"Add":"apple"},
    {"Add":"banana"},
    {"Add":"cherry"},
    {"Remove":"banana"}
  ],
  "count": 4
}
```

### Multi-Node Cluster

Run a two-node cluster to see CRDT synchronization in action:

#### Terminal 1 - Node A
```bash
REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 PEERS=b:127.0.0.1:9002 \
  cargo run -p moirai-network --example awset_node
```

#### Terminal 2 - Node B
```bash
REPLICA_ID=b LISTEN_PORT=9002 HTTP_PORT=8082 PEERS=a:127.0.0.1:9001 \
  cargo run -p moirai-network --example awset_node
```

#### Add items to Node A
```bash
curl -X POST http://localhost:8081/api/op -H "Content-Type: application/json" -d '{"Add":"apple"}'
curl -X POST http://localhost:8081/api/op -H "Content-Type: application/json" -d '{"Add":"banana"}'
```

#### Add items to Node B
```bash
curl -X POST http://localhost:8082/api/op -H "Content-Type: application/json" -d '{"Add":"cherry"}'
```

#### Verify synchronization
Both nodes should have all three items:
```bash
curl http://localhost:8081/api/operations
curl http://localhost:8082/api/operations
```

### Available Operations

**Add an element:**
```bash
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '{"Add":"value"}'
```

**Remove an element:**
```bash
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '{"Remove":"value"}'
```

**Clear the entire set:**
```bash
curl -X POST http://localhost:8081/api/op \
  -H "Content-Type: application/json" \
  -d '"Clear"'
```

### Other HTTP Endpoints

- `GET /api/health` — Health check
- `GET /api/state` — Query computed CRDT state (current set contents)
- `GET /api/operations` — List all operations (history)
- `GET /api/peers` — List connected peers and their status
- `POST /api/pause/<peer_id>` — Simulate network partition
- `POST /api/resume/<peer_id>` — Resume connection and sync
- `POST /api/pause-all` — Pause all peer connections
- `POST /api/resume-all` — Resume all peer connections

### Testing Network Partitions

Simulate a network partition and observe CRDT behavior:

```bash
# Pause connection to node B
curl -X POST http://localhost:8081/api/pause/b

# Make changes on both nodes while partitioned
curl -X POST http://localhost:8081/api/op -H "Content-Type: application/json" -d '{"Add":"isolated_a"}'
curl -X POST http://localhost:8082/api/op -H "Content-Type: application/json" -d '{"Add":"isolated_b"}'

# Resume connection - nodes will auto-sync
curl -X POST http://localhost:8081/api/resume/b

# Both nodes now have both items
curl http://localhost:8081/api/operations
curl http://localhost:8082/api/operations
```
