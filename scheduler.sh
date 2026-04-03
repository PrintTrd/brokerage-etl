#!/bin/sh
set -e
# 24 × 5s = 2 minutes max wait
# RETRIES=0

# Install Docker CLI tools if not already present
if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker CLI..."
    apk add --no-cache docker-cli docker-cli-compose
fi

echo "Waiting for database to be ready..."
sleep "$DB_RETRY_DELAY"

echo "Database is ready! Setting up cron jobs..."
# Uses `docker run` against the already-built etl image so we do not duplicate orchestration logic inside the container.
# `-p``: Project Name
DIR=$(pwd)
echo "*/2 * * * * cd /app && docker compose -p brokerage-etl run --rm -v $DIR/data/input:/app/data/input:ro --no-deps etl" | crontab -
# check log file exists and is writable
# touch /var/log/etl-cron.log
# docker compose exec scheduler ls -l /var/log/etl-cron.log

echo "Cron jobs configured. Starting cron daemon..."
# crond is Daemon (Background Process) that runs the scheduled tasks,
# `-f`: keep it in foreground mode to prevent the container from exiting
# `-l 2`: log to stdout for visibility in `docker logs`
crond -f -l 2
