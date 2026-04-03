#!/bin/bash
echo "==========================================="
echo "Starting Brokerage ETL (Mac/Linux Mode)"
echo "==========================================="

# find current directory and create .env file
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        echo "[INFO] .env not found. Copying from .env.example..."
        cp .env.example .env
    else
        echo "[WARNING] .env.example not found! Creating an empty .env file..."
        touch .env
    fi
fi

# Use sed to remove old HOST_PWD
sed -i.bak '/^HOST_PWD=/d' .env && rm -f .env.bak
echo "HOST_PWD=$(pwd)" >> .env
echo "[INFO] Updated .env with HOST_PWD=$(pwd)"

# Start the database and pgadmin
docker compose up -d db pgadmin
