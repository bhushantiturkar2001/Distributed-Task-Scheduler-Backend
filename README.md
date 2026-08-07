# Distributed Task Scheduler Backend (TaskForge)

A production-grade distributed task scheduling system built with Spring Boot 3.x and Java 17. Schedule one-time or recurring tasks, distribute them across multiple worker nodes, and monitor execution with built-in retry mechanisms and distributed locking.

## 🎯 What Is This?

TaskForge is a **distributed task scheduler** that enables:
- ⏰ Schedule tasks for one-time or recurring execution
- 🔄 Automatic retry with exponential backoff
- 🔒 Distributed locking to prevent duplicate execution
- 📊 Real-time task monitoring and execution history
- 🚀 Horizontal scalability with multiple worker nodes
- 🎯 Priority-based task execution

**Real-world use cases:**
- Send scheduled emails/notifications
- Run periodic data cleanup jobs
- Process batch operations at specific times
- Execute webhooks at scheduled intervals

## 📋 Features Implemented (Sprint 1 ✅)

### ✅ Core Task Management
- **CRUD Operations**: Create, read, update, delete tasks
- **Task Status Tracking**: PENDING → QUEUED → RUNNING → SUCCESS/FAILED
- **Task Filtering**: Filter by status, priority, scheduled time
- **Pagination Support**: Efficient listing of large task sets

### ✅ Database Layer
- **PostgreSQL Integration**: ACID-compliant task storage
- **Flyway Migrations**: Version-controlled schema management
- **3 Main Tables**: tasks, task_schedules, execution_logs
- **Optimized Indexes**: Status + scheduled_at for fast queries

### ✅ REST API
- **OpenAPI/Swagger Documentation**: Auto-generated API docs
- **Request Validation**: Bean validation with detailed error messages
- **Custom Exception Handling**: Proper HTTP status codes (404, 409, 400, 500)
- **DTO Layer**: Clean separation between API contracts and domain models

### ✅ Code Quality
- **14 Unit Tests**: Comprehensive test coverage with JUnit 5 + Mockito
- **Custom Exceptions**: TaskNotFoundException, InvalidTaskOperationException
- **Structured Logging**: SLF4J with contextual information
- **Clean Architecture**: Controller → Service → Repository layers

## 🏗️ Architecture

### Current Architecture (Sprint 1)
```
┌─────────────┐
│  REST API   │ ← Spring Boot 3.2 + OpenAPI/Swagger
└──────┬──────┘
       │
┌──────▼──────┐
│   Service   │ ← Business Logic + Validation
└──────┬──────┘
       │
┌──────▼──────┐
│ Repository  │ ← Spring Data JPA
└──────┬──────┘
       │
┌──────▼──────┐
│ PostgreSQL  │ ← Task Persistence
└─────────────┘
```

### Target Architecture (Sprint 2-6)
```
  React UI  ←──→  REST API  ←──→  PostgreSQL
                     │
                     ▼
                   Kafka  ←──→  Redis (Locks)
                     │
              ┌──────┼──────┐
              ▼      ▼      ▼
          Worker  Worker  Worker
```

## 📊 Database Schema

### tasks
| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| name | VARCHAR(255) | Task name |
| description | TEXT | Optional description |
| payload | JSONB | Task execution data |
| task_type | VARCHAR(50) | HTTP_CALL, LOG, CUSTOM |
| priority | VARCHAR(10) | HIGH, MEDIUM, LOW |
| status | VARCHAR(20) | PENDING, QUEUED, RUNNING, SUCCESS, FAILED, RETRYING, DEAD, CANCELLED |
| scheduled_at | TIMESTAMP | When to execute |
| started_at | TIMESTAMP | Execution start time |
| completed_at | TIMESTAMP | Execution end time |
| retry_count | INT | Current retry attempt |
| max_retries | INT | Max retry attempts |
| error_message | TEXT | Failure reason |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Last update time |

**Indexes:**
- `idx_tasks_status_scheduled` on (status, scheduled_at)
- `idx_tasks_priority` on (priority)

## 🔌 API Endpoints

### Task Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/tasks` | Create a new task |
| `GET` | `/api/v1/tasks` | List all tasks (paginated, filterable) |
| `GET` | `/api/v1/tasks/{id}` | Get task by ID |
| `PUT` | `/api/v1/tasks/{id}` | Update task (only PENDING tasks) |
| `DELETE` | `/api/v1/tasks/{id}` | Delete task (not RUNNING) |
| `PATCH` | `/api/v1/tasks/{id}/cancel` | Cancel task |

### Sample Request: Create Task

```json
POST /api/v1/tasks
{
  "name": "Send Weekly Report",
  "description": "Weekly sales report email",
  "taskType": "HTTP_CALL",
  "priority": "HIGH",
  "scheduledAt": "2026-08-10T09:00:00",
  "maxRetries": 3,
  "payload": "{\"url\":\"https://api.example.com/reports\"}"
}
```

### Sample Response

```json
{
  "id": "a7c3f1e2-4b5d-6789-abcd-ef0123456789",
  "name": "Send Weekly Report",
  "status": "PENDING",
  "priority": "HIGH",
  "scheduledAt": "2026-08-10T09:00:00",
  "retryCount": 0,
  "maxRetries": 3,
  "createdAt": "2026-08-07T14:30:00"
}
```

## 🔧 API Documentation

Once the app is running, access interactive API documentation:

**Swagger UI:** http://localhost:8080/api/swagger-ui.html

Features:
- Try all endpoints directly from the browser
- See request/response schemas
- View validation rules
- Copy cURL commands

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

## 🛠️ Tech Stack

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

## 🚀 Getting Started

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

## 🔧 API Documentation

Once the app is running, access interactive API documentation:

**Swagger UI:** http://localhost:8080/api/swagger-ui.html

Features:
- Try all endpoints directly from the browser
- See request/response schemas
- View validation rules
- Copy cURL commands

## 🧪 Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TaskServiceTest

# Run with coverage report
mvn clean test jacoco:report
```

**Current Test Coverage:**
- ✅ TaskService: 14 unit tests (100% pass rate)
- All CRUD operations tested
- Exception scenarios covered
- Mock-based testing (no DB required)

## 📊 Project Status

### ✅ Sprint 1 Complete (Day 1-7)
- [x] Project setup + folder structure
- [x] Docker Compose (PostgreSQL, Redis, Kafka)
- [x] JPA entities (Task, TaskSchedule, ExecutionLog)
- [x] Flyway migrations
- [x] Repository layer
- [x] Service layer with CRUD
- [x] REST API with validation
- [x] Global exception handling
- [x] DTO layer
- [x] OpenAPI/Swagger documentation
- [x] Unit tests (14 tests)
- [x] Custom exceptions

### 🔜 Upcoming (Sprint 2-6)
- Sprint 2: Kafka integration + Worker architecture
- Sprint 3: Redis locking + Retry mechanism + Priority
- Sprint 4: Recurring tasks (cron) + Metrics API
- Sprint 5: React dashboard
- Sprint 6: Load testing + Deployment

## ⚙️ Environment Configuration

Check `src/main/resources/application.yml` for all configuration options.

**Key Properties:**
- `spring.datasource.url`: PostgreSQL connection
- `spring.data.redis.host`: Redis connection
- `spring.kafka.bootstrap-servers`: Kafka connection
- `server.port`: Application port (default: 8080)
- `server.servlet.context-path`: API base path (default: /api)

## 🐛 Troubleshooting

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

## 💡 Design Decisions

### Why PostgreSQL?
- ACID compliance for reliable task storage
- JSONB for flexible task payloads
- Strong consistency for task status tracking
- Battle-tested at scale

### Why Custom Exceptions?
- **TaskNotFoundException** (404): Clear "not found" semantics
- **InvalidTaskOperationException** (409): State-based operation conflicts
- Better than generic RuntimeException with proper HTTP status codes

### Why DTO Layer?
- Separates API contracts from domain models
- Allows API evolution without breaking database schema
- Validation at API boundary, not domain layer
- Clean mapping with `toEntity()` and `fromEntity()`

### Current Trade-offs
- **No distributed locking yet**: Single-instance execution (Sprint 3)
- **No Kafka yet**: Synchronous task scheduling (Sprint 2)
- **No retry mechanism yet**: Manual retry only (Sprint 3)

## 🤝 Contributing

This is a learning/portfolio project. Feel free to:
- Report issues
- Suggest improvements
- Fork and experiment

## 📚 References

- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [Apache Kafka](https://kafka.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [OpenAPI/Swagger](https://springdoc.org/)

---

**Project:** TaskForge - Distributed Task Scheduler  
**Created:** July 15, 2026  
**Last Updated:** August 7, 2026  
**Sprint Status:** Sprint 1 Complete ✅
