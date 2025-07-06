# Tackboard

📝 **Homework Assignment: “Tackboard” – A Minimal Async Pub/Sub System**

## 🧠 Goal
Build a minimal in-memory publish/subscribe system over TCP where multiple clients can:
- Subscribe to a topic
- Publish a message to a topic
- Receive messages on topics they subscribed to

## 📦 Project Overview
**Components:**
- **tackboardlib**: A library with:
  - Topic/message definitions
  - Serialization logic
  - Server data structures and logic
- **tackboardsrv**: A binary that runs the pub/sub server
- **tackboardctl**: A CLI client that connects, subscribes, and publishes

## 💡 Features to Implement
### Server
- Accepts multiple clients via TCP
- Keeps an in-memory `HashMap<Topic, Vec<Sender<Message>>>`
- On Publish, sends to all subscribed clients
- On Subscribe, registers the client for a topic

### Client
- Connects via TCP
- Can send `Subscribe { topic }` or `Publish { topic, message }` requests
- Listens to incoming messages and prints them

## 📚 Concepts You’ll Practice
| Skill                        | Covered |
|------------------------------|:-------:|
| tokio async I/O              |   ✅    |
| mpsc channels                |   ✅    |
| HashMap with borrowed keys   |   ✅    |
| Traits + impl for custom types|  ✅    |
| Cloning, borrowing, ownership|   ✅    |
| serde for JSON serialization |   ✅    |
| Custom enums (ClientRequest, ServerMessage) | ✅ |
| Testing with #[tokio::test]  |   ✅    |

## 🧪 Bonus Challenges
- Add timeouts: Disconnect clients who are idle for 30s
- Support unsubscribing
- Implement wildcard topic matching
- Write integration tests

## 🧱 Suggested Message Types
```rust
#[derive(Serialize, Deserialize)]
pub enum ClientRequest {
    Subscribe { topic: String },
    Publish { topic: String, message: String },
}

#[derive(Serialize, Deserialize)]
pub enum ServerMessage {
    Message { topic: String, message: String },
    Ack,
    Error(String),
}
```

## 🧰 Crate Suggestions
- `tokio` – async networking
- `tokio_util::codec::LengthDelimitedCodec`
- `tokio_serde` – serialize messages over TCP
- `serde`, `serde_json`
- `clap` for client CLI

## 🔖 Project Structure
```
tackboard/
├── Cargo.toml (workspace)
├── tackboardlib/
│   └── src/lib.rs
├── tackboardsrv/
│   └── src/main.rs
└── tackboardctl/
    └── src/main.rs
```

## ✅ Deliverables
- Clients can connect to the server
- Subscribed clients receive published messages
- You’ve implemented proper error handling and logging
- Bonus if you add tests or make it robust to client disconnects

