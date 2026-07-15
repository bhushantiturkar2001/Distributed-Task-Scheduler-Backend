# Distributed Task Scheduler Backend

Production-grade distributed task scheduling system built with Spring Boot 3.x and Java 17.

## Project Structure

```
distributed-task-scheduler-backend/
├── src/
│   ├── main/
│   │   ├── java/com/taskforge/
│   │   │   ├── DistributedTaskSchedulerApplication.java
│   │   │   ├── config/           (Configuration classes)
│   │   │   ├── model/            (JPA Entities)
│   │   │   ├── repository/       (Spring Data Repositories)
│   │   │   ├── service/          (Business Logic)
│   │   │   ├── controller/       (REST Controllers)
│   │   │   └── kafka/            (Kafka Producers & Consumers)
│   │   └── resources/
│   │       ├── application.yml   (Configuration)
│   │       └── db/migration/     (Flyway Migrations)
│   └── test/
│       └── java/com/taskforge/   (Tests)
├── pom.xml                        (Maven Dependencies)
├── docker-compose.yml             (Local Development Environment)
└── README.md
```

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Language** | Java | 17 |
| **Framework** | Spring Boot | 3.2.0 |
| **Database** | PostgreSQL | 15 |
| **Cache/Lock** | Redis | 7 |
| **Message Queue** | Kafka | 7.5.0 |
| **ORM** | Hibernate/JPA | Spring Data |
| **Build Tool** | Maven | 3.9+ |

## Prerequisites

- Java 17+
- Maven 3.9+
- Docker & Docker Compose

## Getting Started

### 1. Start Infrastructure (Docker)

```bash
docker-compose up -d
```

This starts:
- PostgreSQL (localhost:5432)
- Redis (localhost:6379)
- Zookeeper (localhost:2181)
- Kafka (localhost:9092)

### 2. Verify Containers are Running

```bash
docker-compose ps
```

Expected output: All services should be "Up"

### 3. Build the Project

```bash
mvn clean install
```

### 4. Run the Application

```bash
mvn spring-boot:run
```

Or run via IDE: Right-click `DistributedTaskSchedulerApplication` → Run

### 5. Verify Application is Running

- Open: http://localhost:8080/api/swagger-ui.html
- Should see Swagger UI with API documentation

### 6. Check Logs

```bash
# Check PostgreSQL
docker logs taskforge-postgres

# Check Redis
docker logs taskforge-redis

# Check Kafka
docker logs taskforge-kafka
```

## Testing Database Connection

Once app is running, you can test connections:

```bash
# PostgreSQL
psql -h localhost -U postgres -d taskforge

# Redis
redis-cli -h localhost

# Kafka
docker exec taskforge-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Environment Configuration

Check `src/main/resources/application.yml` for all configuration options.

**Key Properties:**
- `spring.datasource.url`: PostgreSQL connection
- `spring.data.redis.host`: Redis connection
- `spring.kafka.bootstrap-servers`: Kafka connection
- `server.port`: Application port (default: 8080)
- `server.servlet.context-path`: API base path (default: /api)

## Troubleshooting

### Port Already in Use
```bash
# Kill process on port 8080
lsof -ti:8080 | xargs kill -9  # macOS/Linux

# Or change port in application.yml
server.port: 8081
```

### Docker Issues
```bash
# Clean up all containers
docker-compose down -v

# Restart
docker-compose up -d
```

### Database Connection Error
Ensure PostgreSQL is healthy:
```bash
docker-compose logs postgres
```

## References

- Spring Boot: https://spring.io/projects/spring-boot
- Kafka: https://kafka.apache.org/
- PostgreSQL: https://www.postgresql.org/

---
