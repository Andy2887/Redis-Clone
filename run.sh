# Exit early if any commands fail
set -e

(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  mvn -q -B package -Ddir=/tmp/codecrafters-build-redis-java
)

exec java -jar /tmp/codecrafters-build-redis-java/codecrafters-redis.jar "$@"
