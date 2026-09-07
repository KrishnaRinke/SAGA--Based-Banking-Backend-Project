# Secure Digital Banking & Fraud Detection Platform

A microservices-based digital banking platform built with Java and Spring Boot, demonstrating event-driven transaction processing, real-time fraud detection, distributed service communication, payment integration, and asynchronous notifications.

## Architecture

```text
                         ┌──────────────────┐
                         │      Client      │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   API Gateway    │
                         │      :8080       │
                         └────────┬─────────┘
                                  │
             ┌────────────────────┼────────────────────┐
             ▼                    ▼                    ▼
      ┌─────────────┐      ┌──────────────┐     ┌─────────────┐
      │   Account   │      │ Transaction  │     │   Payment   │
      │   Service   │      │   Service    │     │   Service   │
      │    :8081    │      │    :8082     │     │    :8083    │
      └──────┬──────┘      └──────┬───────┘     └──────┬──────┘
             │                    │                    │
             │                    ▼                    │
             │             ┌─────────────┐             │
             │             │    Kafka    │             │
             │             │ Event Bus   │             │
             │             └──────┬──────┘             │
             │                    │                    │
             │          ┌─────────┴──────────┐         │
             │          ▼                    ▼         │
             │   ┌──────────────┐    ┌──────────────┐ │
             │   │    Fraud     │    │ Notification │ │
             │   │   Detection  │    │   Service    │ │
             │   │    :8084     │    │    :8085     │ │
             │   └──────┬───────┘    └──────────────┘ │
             │          │                              │
             │          ▼                              ▼
             │       Redis                         Razorpay
             │
             ▼
           MySQL
```

## Microservices

### API Gateway — `8080`
- Centralized entry point
- Request routing
- CORS configuration
- Redis-backed rate-limiting support

### Account Service — `8081`
- Account management
- Balance management
- Account status
- Transaction event consumption
- MySQL persistence

### Transaction Service — `8082`
- Transaction creation and validation
- Transaction status management
- OpenFeign communication
- Kafka event publishing
- Transaction tracking

### Payment Service — `8083`
- Payment order creation
- Payment status management
- Razorpay integration
- Payment success/failure handling
- Payment event publishing

### Fraud Detection Service — `8084`
- Rule-based fraud detection
- Transaction amount analysis
- Balance validation
- Redis-based transaction velocity checks
- Suspicious transaction identification

### Notification Service — `8085`
- Kafka event consumption
- Asynchronous notification processing
- Email notification support

## Transaction Flow

```text
Client
  │
  ▼
API Gateway
  │
  ▼
Transaction Service
  │
  ├── Validate transaction
  ├── Check account information
  │
  ▼
Kafka
  │
  ▼
Fraud Detection
  │
  ├── Amount check
  ├── Balance check
  └── Velocity check using Redis
  │
  ├───────────────┐
  ▼               ▼
 SAFE          SUSPICIOUS
  │               │
  │               ▼
  │         OTP Verification
  │
  ▼
Transaction Processing
  │
  ▼
Account Update
  │
  ▼
Kafka Events
  │
  ▼
Notification Service
```

## Key Features

- Microservices-based architecture
- Apache Kafka event-driven communication
- Redis-based fraud velocity tracking
- OTP verification with Redis TTL
- Rule-based fraud detection
- Transaction state management
- Compensation/refund handling for failed or suspicious flows
- Razorpay payment integration
- API Gateway
- OpenFeign synchronous service communication
- Database-per-service approach
- Asynchronous notifications
- Dockerized Kafka, Zookeeper, and Redis
- Spring Boot Actuator health checks

## Technology Stack

| Technology | Purpose |
|---|---|
| Java | Programming language |
| Spring Boot | Microservices |
| Spring Cloud Gateway | API Gateway |
| OpenFeign | Synchronous service communication |
| Apache Kafka | Event-driven communication |
| Redis | Velocity tracking and OTP TTL |
| MySQL | Persistent storage |
| Razorpay | Payment processing |
| Docker Compose | Infrastructure |
| Spring Data JPA | Database access |
| Spring Boot Actuator | Health monitoring |
| Maven | Build management |

## Data Storage

The services follow a database-per-service approach:

```text
Account Service       → account_db
Transaction Service   → transaction_db
Payment Service       → payment_db
```

## Running Locally

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:

- Kafka
- Zookeeper
- Redis

### 2. Start MySQL

Make sure MySQL is running locally and the required credentials match each service configuration.

### 3. Configure Razorpay

Never commit Razorpay credentials to GitHub.

Configure these environment variables:

```text
RAZORPAY_KEY_ID
RAZORPAY_KEY_SECRET
RAZORPAY_WEBHOOK_SECRET
```

Example Spring configuration:

```yaml
razorpay:
  key-id: ${RAZORPAY_KEY_ID}
  key-secret: ${RAZORPAY_KEY_SECRET}
  webhook-secret: ${RAZORPAY_WEBHOOK_SECRET}
```

In IntelliJ IDEA:

```text
Run → Edit Configurations → Environment variables
```

### 4. Start Services

Start the services from IntelliJ IDEA:

```text
API Gateway          → 8080
Account Service      → 8081
Transaction Service  → 8082
Payment Service      → 8083
Fraud Detection      → 8084
Notification Service → 8085
```

## Health Checks

Spring Boot Actuator provides service health information.

Example:

```text
http://localhost:8081/actuator/health
```

## Security

Sensitive credentials should be supplied through environment variables and excluded from source control.

Recommended `.gitignore` entries:

```gitignore
target/
node_modules/
.env
.env.*
!.env.example
.idea/
*.iml
```

## Future Improvements

- JWT authentication and role-based authorization
- Kafka retry and dead-letter topics
- Idempotent event processing
- Resilience4j circuit breakers
- Distributed tracing
- Prometheus and Grafana monitoring
- Kubernetes deployment
- AWS cloud deployment
- Automated integration testing
- Advanced fraud scoring

## Author

**Krishna Rinke**

Backend-focused project built to explore microservices, event-driven architecture, distributed transaction processing, fraud detection, and payment integration.
