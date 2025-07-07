# Tackboard

![Homework Score](https://img.shields.io/badge/homework-10%2F10-brightgreen)

> **Note:** The code in this repository was written by me as part of a self-learning exercise. The task was defined and the final implementation evaluated using generative AI (OpenAI’s ChatGPT). The project was used to improve my understanding of async networking, ownership, and message-passing in Rust.



📝 **Homework Assignment: “Tackboard” – A Minimal Async Pub/Sub System**

## 🧠 Homework Evaluation

| Criteria                                      | Rating     |
|----------------------------------------------|------------|
| Async TCP Server/Client using Tokio           | ✅ Excellent use of `tokio`, `tokio_util`, and `tokio_serde` |
| Framed message serialization/deserialization  | ✅ Well-structured with clear separation between client/server |
| Ownership, Borrowing, Locking                 | ✅ Shows strong grasp; recent improvements removed contention |
| Handling subscriptions and topic storage      | ✅ Correct and scalable |
| Lock contention avoidance                     | ✅ Significantly improved by cloning before `.await` |
| Correct use of `Arc<Mutex<...>>`              | ✅ Used appropriately across shared state |
| Clean ClientRequest / ServerResponse Enums    | ✅ Nicely designed and extensible |
| CLI Client experience                         | ✅ `clap`-based design is clean and effective |
| Logging and diagnostics                       | ✅ Logging is detailed and context-aware |
| Code structure and readability                | ✅ Modular and maintainable |
| Bonus: Publish broadcast and topic history    | ✅ Implemented well |
| Bonus: Connection deduplication               | ✅ Clients are not re-registered if already connected |

**✅ Final Score: 10/10 — Excellent work!**  
Your implementation shows strong understanding of async concurrency, framing, message routing, and Rust’s ownership model. Clear structure, correct locking patterns, and thoughtful logging elevate this beyond a minimal pub/sub system.

---

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

