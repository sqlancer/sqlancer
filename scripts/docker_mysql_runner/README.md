# 🐳 Dockerized SQLancer Runner (MySQL)

This script automates SQLancer fuzz testing on a Dockerized MySQL instance.

## 🚀 Features
- Builds & runs a MySQL container using Docker
- Waits for the DB to initialize
- Launches SQLancer to fuzz test the MySQL instance
- Cleans up the container after test run

## 📂 Folder Structure

<pre lang="markdown"> 
sqlancer/ 
├── scripts/ 
│ └── docker_mysql_runner/ │ 
├── Dockerfile.mysql 
│ ├── run_sqlancer_mysql.sh 
│ └── README.md </pre>

## 📦 Prerequisites
- Java 17+
- Maven
- Docker
- Git Bash (for Windows)

## ▶️ How to Run

```bash
cd scripts/docker_mysql_runner
./run_sqlancer_mysql.sh

        

