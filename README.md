# Redis Server Implementation in Java

A high-performance, thread-safe Redis server implementation built from scratch in Java. This project demonstrates fundamental concepts of network programming, data structures, and concurrent systems design.

## 🚀 Features

### Core Commands
- **Connection Management**: `PING` - Test server connectivity
- **String Operations**: 
  - `SET key value [PX milliseconds]` - Store key-value pairs with optional expiry
  - `GET key` - Retrieve values by key
  - `ECHO message` - Echo back messages
- **List Operations**:
  - `RPUSH key element [element ...]` - Add elements to the right of a list
  - `LPUSH key element [element ...]` - Add elements to the left of a list
  - `LPOP key [count]` - Remove and return elements from the left of a list
  - `BLPOP key timeout` - Blocking left pop with timeout support
  - `LRANGE key start stop` - Get a range of elements from a list
  - `LLEN key` - Get the length of a list

### Advanced Features
- **Replication**: Master-replica support with command propagation and full resynchronization
- **Expiry Support**: Automatic key expiration with millisecond precision
- **Blocking Operations**: BLPOP with configurable timeouts and FIFO client ordering
- **Thread Safety**: Concurrent client handling with proper synchronization
- **RESP Protocol**: Full Redis Serialization Protocol compliance
- **Memory Management**: Automatic cleanup of empty data structures

## 🛠️ Setup and Installation

### Prerequisites
- Java 8 or higher
- Maven 3.6 or higher

### Installation
1. Clone the repository

2. Start the project:
   ```bash
   ./run.sh
   ```


The server will start on `localhost:6379` by default.

## 📖 Usage

### Basic Operations
```bash
# Connect using redis-cli or any Redis client
redis-cli -p 6379

# Test connectivity
> PING
PONG

# String operations
> SET mykey "Hello World"
OK
> GET mykey
"Hello World"

# Set with expiry (in milliseconds)
> SET temp_key "expires soon" PX 5000
OK

# List operations
> RPUSH mylist "first" "second" "third"
(integer) 3
> LRANGE mylist 0 -1
1) "first"
2) "second"
3) "third"

# Blocking operations
> BLPOP mylist 10
1) "mylist"
2) "first"
```

## 🪞 Replication

This server supports Redis master-replica replication:

- Start a replica with:
  ```bash
  ./run.sh --port 6380 --replicaof "localhost 6379"

### Supported Data Types
- **Strings**: UTF-8 encoded text values
- **Lists**: Ordered collections of strings with O(1) head/tail operations

## 🏗️ Architecture

### Core Components
- **Main.java**: Server initialization and client connection handling
- **HandleClient.java**: Individual client request processing and command execution
- **BlockedClient.java**: Manages blocking operations and timeout handling

### Design Patterns
- **Multi-threaded Server**: Each client connection runs in its own thread
- **Command Pattern**: Individual handler methods for each Redis command
- **Observer Pattern**: Blocked clients are notified when list elements become available
- **Thread-safe Collections**: ConcurrentHashMap for shared state management

## 🧪 Testing

Test the server using any Redis client:
```bash
# Using redis-cli
redis-cli -p 6379

# Using telnet for raw RESP protocol
telnet localhost 6379
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
4. Ensure all tests pass: `mvn test`
5. Commit your changes: `git commit -am 'Add some feature'`
6. Push to the branch: `git push origin feature-name`
7. Submit a pull request

### Development Guidelines
- Follow Java naming conventions
- Add comprehensive logging for debugging
- Ensure thread safety for shared resources
- Write clear, self-documenting code
- Add unit tests for new features

## 📋 Project Structure
```
src/
├── main/
│   └── java/
│       ├── Main.java          # Server entry point
│       ├── HandleClient.java  # Client request handler
│       └── BlockedClient.java # Blocking operation support
└── test/
    └── java/                  # Unit tests
```

## 🔮 Future Enhancements
- Persistence support (RDB)
- Transactions
- Replication
- Fix the BLPOP command bug
- Fix the XREAD BLOCK command bug

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 🙏 Acknowledgements

This project follows the [Codecrafters tutorial "Build Your Own Redis"](https://codecrafters.io/challenges/redis).
