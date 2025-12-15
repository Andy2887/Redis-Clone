# Redis Clone in Java

A high-performance, thread-safe Redis server implementation built from scratch in Java.

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
│       │   └── RdbWriter.java       # Helper for writing RDB file
│       └── StorageManager/
│           ├── BlockedClient.java   # Data structure for blocked client state (BLPOP/XREAD)
│           ├── ListStorage.java     # Thread-safe Redis list implementation with blocking support
│           ├── RESPProtocol.java    # RESP protocol parsing and formatting utilities
│           ├── StreamEntry.java     # Data structure for Redis stream entries
│           ├── StreamIdHelper.java  # Stream ID parsing, validation, and comparison
│           ├── StreamStorage.java   # Thread-safe Redis stream implementation with blocking support
│           └── StringStorage.java   # Thread-safe Redis string implementation with expiry support
```

## 🚀 Features

### Core Commands
- **Connection Management**: `PING` - Test server connectivity
- **Configuration**: 
  - `CONFIG GET <param>` - Retrieve server configuration parameters (`dir`, `dbfilename`)
- **Role Management**:
  - `REPLICAOF NO ONE` - Switch server to master role
- **Persistence**:
  - `SAVE` - Perform a synchronous save of the dataset to an RDB file
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
  - `XREAD [BLOCK timeout] streams key1 [key2 ...] id1 [id2 ...]` - Read new entries from one or more streams, optionally blocking
- **Key Operations**:
  - `TYPE key` - Check the data type of a key (returns: string, list, stream, or none)
  - `KEYS *` - List all keys in the database
  - `INCR key` - Increment the integer value of a key by 1
- **Server Information**:
  - `INFO replication` - Get information about replication status
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
> XADD stream_key 1526919030474-0 temperature 36 humidity 95
"1526919030474-0"
> XADD stream_key * foo bar
# Note: When you use redis-cli in shell, you should type "*" instead of *.
# If you use *, The shell is expanding the * wildcard to match all files in your current directory.
# This is a shell expansion issue, not a problem with my code. The * is being interpreted by shell before it reaches redis-cli
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
The replica will connect to the master, perform the handshake, and receive command propagation.

## 💾 RDB Persistence

This server supports loading data from Redis RDB files at startup.

**Example:**
```bash
./server.sh --dir /path/to/rdb/dir --dbfilename dump.rdb
```
After loading, all keys and values from the RDB file are available for `GET`, `KEYS *`, and other commands.

Note: Currently, my Redis Clone only Supports reading String data type from RDB files.

## 🧪 Testing

This project includes a comprehensive test suite using JUnit 5 to ensure reliability and correctness.

### Running Tests

```bash
# Run all tests
mvn test

# Run tests quietly (less verbose output)
mvn test -q

# Run a specific test class
mvn test -Dtest=StringStorageTest

# Run tests with coverage
mvn clean test
```

### Test Coverage

Currently implemented tests:

#### **StringStorage Tests** (42 tests)
- **Basic Operations**: SET, GET, overwriting values, multiple keys
- **Expiry Management**: Time-based expiration with millisecond precision
- **Key Operations**: EXISTS, REMOVE, SIZE, getAllKeys
- **Expiry APIs**: setExpiry, removeExpiry, getExpiryTime, cleanupExpiredKeys
- **Edge Cases**: Empty strings, long values, various key formats
- **Concurrency**: Multi-threaded operations, race conditions, thread safety
- **Stress Tests**: 1,000+ keys with varying expiry times

### Writing New Tests

Test files are located in `src/test/java/`. To add new tests:

1. Create a test class in the appropriate package under `src/test/java/`
2. Use JUnit 5 annotations (`@Test`, `@BeforeEach`, `@DisplayName`)
3. Follow the existing test patterns in `StringStorageTest.java`
4. Run `mvn test` to verify your tests pass

### Future Test Plans

Planned test coverage for:
- List Operations (LPUSH, RPUSH, LPOP, BLPOP, LRANGE)
- Stream Operations (XADD, XRANGE, XREAD with blocking)
- Transaction Operations (MULTI, EXEC, DISCARD)
- Replication (Master-replica sync, command propagation)
- RDB Persistence (Loading, saving, expiry handling)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
4. Ensure all tests pass: `mvn test`
5. Commit your changes: `git commit -am 'Add some feature'`
6. Push to the branch: `git push origin feature-name`
7. Submit a pull request

## 🔮 Future Enhancements
- Enhanced `REPLICAOF` command: allow dynamic switching between master and replica roles
- Full sync: enable replicas to receive and load real RDB files from master

## 🙏 Acknowledgements

This project is implemented following the tutorial from Codecrafters [Build Your Own Redis](https://app.codecrafters.io/courses/redis/overview). Special thanks to the Codecrafters team for their excellent resources and guidance.

I also use GitHub Copilot as a learning tool while building this project. This approach helps me learn by doing, while still benefiting from Copilot’s guidance on structure and best practices.

To use Copilot to guide rather than complete tasks for me, I add the following prompt to the end of my requests:

```
Please explain the overall architecture and key steps involved, including:

- The functions or modules that should be created
- The libraries or system-level calls that are typically involved, with code examples showing how they are used
- The important considerations to keep in mind when building it

Please don’t provide a full implementation—just guide me through the architecture, logic, and relevant technical concepts. I will write the code myself.
```