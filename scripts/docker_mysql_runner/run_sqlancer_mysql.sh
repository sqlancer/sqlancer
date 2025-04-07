#!/bin/bash

echo "🚀 Starting Dockerized MySQL container..."
docker run -d --rm --name mysql-sqlancer -p 3307:3306 sqlancer-mysql

echo "⏳ Waiting for MySQL to be ready..."
sleep 20

echo "🧪 Running SQLancer fuzz testing on MySQL..."
cd ../../
mvn exec:java -Dexec.mainClass="sqlancer.MySQLDriver" \
  -Dexec.args="--host 127.0.0.1 --port 3307 --user root --password root"

echo "🧼 Cleaning up Docker container..."
docker stop mysql-sqlancer
