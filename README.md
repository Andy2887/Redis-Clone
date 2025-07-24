# Redis Server Implementation in Java

A high-performance, thread-safe Redis server implementation built from scratch in Java. This project demonstrates fundamental concepts of network programming, data structures, and concurrent systems design.

## 🚀 Features

### Core Commands
- **Connection Management**: `PING` - Test server connectivity
- **Configuration**: 
  - `CONFIG GET <param>` - Retrieve server configuration parameters (`dir`, `dbfilename`)
- **Role Management**:
  - `REPLICAOF NO ONE` - Switch server to master role
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
- **Stream Operations**:
  - `XADD key id field value [field value ...]` - Add entries to a stream
  - `XRANGE key start end` - Get a range of entries from a stream
  - `XREAD [BLOCK timeout] streams key [key ...] id [id ...]` - Read new entries from one or more streams, optionally blocking
- **Transaction Support**:
  - `MULTI` - Start a transaction block
  - `EXEC` - Execute all queued commands in the transaction
  - `DISCARD` - Discard all queued commands in the transaction

### Advanced Features
- **Replication**: Master-replica support with command propagation and full resynchronization
- **RDB Persistence**: Loads data from RDB files at startup (`--dir` and `--dbfilename` flags supported). Supports parsing multiple keys, string values, and expiry times from RDB files.
- **Expiry Support**: Automatic key expiration with millisecond precision
- **Blocking Operations**: BLPOP and XREAD BLOCK with configurable timeouts and FIFO client ordering
- **Thread Safety**: Concurrent client handling with proper synchronization

## 🛠️ Setup and Installation

### Prerequisites
- Java 8 or higher (Java 23+ recommended)
- Maven 3.6 or higher

### Installation
1. Clone the repository

2. Start the project:
   ```bash
   ./server.sh
   ```

The server will start on `localhost:6379` by default.

## 📖 Basic Operations

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

# Stream operations
> XADD mystream * field1 value1
"1680000000000-0"
> XRANGE mystream - +
1) 1) "1680000000000-0"
   2) 1) "field1"
      2) "value1"
> XREAD BLOCK 5 streams mystream 0-0
1) 1) "mystream"
   2) 1) 1) "1680000000000-0"
         2) 1) "field1"
               2) "value1"

# Transaction operations
> MULTI
OK
> SET key1 "value1"
QUEUED
> RPUSH mylist "item"
QUEUED
> EXEC
1) OK
2) (integer) 1
```

## 🪞 Replication

This server supports Redis master-replica replication.

#### Start a replica with:

Option 1:
```bash
./server.sh --port 6380 --replicaof "localhost 6379"
```
Option 2:
```bash
./server.sh --port 6380 --replicaof 127.0.0.1 6379
```
- The replica will connect to the master, perform the handshake, and receive command propagation.

## 💾 RDB Persistence

This server supports loading data from Redis RDB files at startup.

- Use the `--dir <dir>` and `--dbfilename <filename>` flags to specify the RDB file location.
- On startup, the server parses the RDB file and loads all string keys, values, and expiry times into memory.
- Expired keys (at load time) are ignored.
- Only length-prefixed string encodings are supported for RDB parsing.

**Example:**
```bash
./server.sh --dir /path/to/rdb/dir --dbfilename dump.rdb
```
After loading, all keys and values from the RDB file are available for `GET`, `KEYS *`, and other commands.

### Supported Data Types
- **Strings**: UTF-8 encoded text values
- **Lists**: Ordered collections of strings with O(1) head/tail operations
- **Streams**: Append-only log data structure for time-series and messaging

## 🏗️ Architecture

### Core Components
- **Main.java**: Server initialization and client connection handling
- **HandleClient.java**: Individual client request processing and command execution
- **HandleReplica.java**: Handles replica handshake and command propagation from master
- **StorageManager/**: Contains storage classes for strings, lists, streams, and blocking client management

### Design Patterns
- **Multi-threaded Server**: Each client connection runs in its own thread
- **Command Pattern**: Individual handler methods for each Redis command
- **Observer Pattern**: Blocked clients are notified when list or stream elements become available
- **Thread-safe Collections**: Uses `ConcurrentHashMap` and other thread-safe structures for shared state

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
│       ├── Main.java                # Server entry point: starts server, loads RDB, handles config
│       ├── HandleClient.java        # Handles client connections, RESP parsing, command execution
│       ├── HandleReplica.java       # Handles replica handshake and command propagation from master
│       ├── RdbManager/
│       │   ├── RdbStringResult.java # Helper for RDB string parsing
│       │   └── RdbSizeResult.java   # Helper for RDB size parsing
│       └── StorageManager/
│           ├── BlockedClient.java   # Data structure for blocked client state (BLPOP/XREAD)
│           ├── ListStorage.java     # Thread-safe Redis list implementation with blocking support
│           ├── RESPProtocol.java    # RESP protocol parsing and formatting utilities
│           ├── StreamEntry.java     # Data structure for Redis stream entries
│           ├── StreamIdHelper.java  # Stream ID parsing, validation, and comparison
│           ├── StreamStorage.java   # Thread-safe Redis stream implementation with blocking support
│           └── StringStorage.java   # Thread-safe Redis string implementation with expiry support
└── test/
    └── java/                        # Unit tests
```

## 🔮 Future Enhancements
- Enhanced `REPLICAOF` command: allow dynamic switching between master and replica roles
- Full sync: enable replicas to receive and load real RDB files from master
- Implement `SAVE` command for manual persistence

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 🙏 Acknowledgements

This project is implemented following the tutorial from Codecrafters "Build Your Own Redis". Special thanks to the Codecrafters team for their excellent resources and guidance.