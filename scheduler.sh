#!/bin/sh
set -e # Exit immediately if a command exits with a non-zero status

# Install Docker CLI tools if not already present
if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker CLI..."
    apk add --no-cache docker-cli docker-cli-compose
fi

sleep "$DELAY"
DIR=$(pwd)
# Uses `docker run` against the already-built etl image to execute the ETL script every 2 minutes.
# `-p``: Project Name
echo "*/2 * * * * cd /app && docker compose -p brokerage-etl run --rm -v $DIR/data/input:/app/data/input:ro --no-deps etl" | crontab -
echo "Cron jobs configured. Starting cron daemon..."
# crond is Daemon (Background Process) that runs the scheduled tasks,
# `-f`: keep it in foreground mode to prevent the container from exiting
# `-l 2`: log to stdout for visibility in `docker logs`
crond -f -l 2
