# FlowForge — Visual ETL Pipeline Builder

A drag-and-drop ETL pipeline builder with real-time execution monitoring. Build data pipelines visually, execute them against real S3/Kafka/Postgres endpoints, and watch each node's status update live.

## Architecture

          ```
          ┌─────────────┐     REST/WS      ┌──────────────────────────────────────────────┐
          │  React      │ ◄──────────────► │  Spring Boot 3.2 (port 8080)                 │
          │  Frontend   │                  │  ┌───────────┐  ┌──────────┐  ┌───────────┐  │
          │  (port 3000)│                  │  │ Pipeline  │  │Execution │  │  AI       │  │
          └─────────────┘                  │  │ CRUD API  │  │Orchestr. │  │  Service  │  │
                                           │  └───────────┘  └──────────┘  └───────────┘  │
                                           └───────┬──────────────────┬───────────────┬───┘
                                                   |                  │               │
                                           ┌───────|──────────────────┼───────────────┼────────────────┐
                                           │       │                  |               │                │
                                           │  ┌────▼────┐        ┌────▼──────┐  ┌─────▼───┐  ┌────────┐│
                                           │  │Postgres │        │ RabbitMQ  │  │  Kafka  │  │ Redis  ││
                                           │  │(pipeline│        │(task      │  │(data    │  │(status ││
                                           │  │ store)  │        │ dispatch) │  │ plane)  │  │ cache) ││
                                           │  └─────────┘        └───────────┘  └─────────┘  └────────┘│
                                           │                                                           │
                                           │ ┌───────────┐  ┌──────────────────┐  ┌────────────────┐   │
                                           │ │LocalStack │  │  Flink Cluster   │  │  Ollama        │   │
                                           │ │(S3+Dynamo)│  │ (filter/aggregate│  │ (AI pipeline   │   │
                                           │ └───────────┘  │  JAR jobs)       │  │  generation)   │   │
                                           │                └──────────────────┘  └────────────────┘   │
                                           │                                                           │
                                           └───────────────────────────────────────────────────────────┘
```

**Control plane**: RabbitMQ dispatches `TaskMessage` objects to per-node-type queues. Each executor worker consumes its queue, processes data, and publishes a `CompletionEvent`.

**Data plane**: Kafka topics named `flowforge.{executionId}.{sourceNodeId}_to_{targetNodeId}` carry records between nodes as JSON strings.

**State**: Redis stores execution status (`pipeline:{execId}:status`) and per-node status hashes. DynamoDB (via LocalStack) stores immutable execution audit records. STOMP WebSocket pushes live updates to the frontend.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- 8 GB RAM recommended (Flink + Kafka + all services)
- Ports free: 3000, 8080, 5432, 5672, 6379, 9092, 8081, 4566, 11434, 15672

## Quick Start

```bash
# 1. Clone / navigate to the project
cd flowforge

# 2. Start all services
docker compose up --build -d

# 3. Wait ~60s for services to initialise, then seed test data
bash scripts/init-localstack.sh   # creates S3 buckets + DynamoDB table
bash scripts/seed-data.sh         # uploads sales.csv and products.json to S3

# 4. Open the UI
open http://localhost:3000
```

The frontend loads with a demo pipeline (S3 Source → Filter → Postgres Sink) pre-populated on the canvas.

## Using FlowForge

### Building a Pipeline

1. **Drag** node types from the left palette onto the canvas
2. **Connect** nodes by dragging from a node's right handle to another's left handle
3. **Click** a node to open the config panel on the right and fill in its properties
4. **Name** the pipeline in the top bar and click **Save**

### Running a Pipeline

1. Save the pipeline first (a pipeline ID is required before execution)
2. Click **Run Pipeline** — the execution monitor replaces the config panel
3. Node status badges update live: grey (waiting) → amber pulse (running) → green (done) / red (failed)
4. Execution history is shown at the bottom of the monitor panel

### AI Pipeline Generation

Type a plain-English description in the bottom bar and click **Generate**:

> *"Read sales.csv from S3 bucket flowforge-data, filter rows where amount > 500, write to Postgres table big_sales"*

The AI service calls Ollama (or falls back to a demo pipeline if Ollama isn't available).

## Node Types

| Category | Node | Description |
|----------|------|-------------|
| Source | S3 File Source | Read CSV or JSON from an S3 bucket |
| Source | Kafka Topic | Consume messages from a Kafka topic |
| Transform | Filter | Keep rows matching a field predicate (`==`, `!=`, `>`, `<`, `contains`) |
| Transform | Field Mapper | Rename or select fields (configurable from/to pairs) |
| Transform | Aggregator | Group by field and apply count / sum / avg |
| Sink | PostgreSQL | Write rows to a Postgres table (append or upsert) |
| Sink | S3 Output | Write CSV or JSON to an S3 key prefix |
| Sink | Kafka Output | Publish records to a Kafka topic |

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| Frontend | http://localhost:3000 | — |
| Backend API | http://localhost:8080/api | — |
| RabbitMQ UI | http://localhost:15672 | guest / guest |
| Flink UI | http://localhost:8081 | — |
| LocalStack | http://localhost:4566 | — |

## API Reference

```
GET    /api/pipelines              List all pipelines
POST   /api/pipelines              Create pipeline  { name, description, definition }
GET    /api/pipelines/{id}         Get pipeline
PUT    /api/pipelines/{id}         Update pipeline
DELETE /api/pipelines/{id}         Delete pipeline
POST   /api/pipelines/{id}/execute Start execution → { executionId }
GET    /api/pipelines/{id}/status?executionId=...  Execution status + per-node statuses
GET    /api/pipelines/{id}/history Last 10 execution records (from DynamoDB)
GET    /api/node-types             List supported node types with config schemas
POST   /api/ai/generate-pipeline   { prompt } → pipeline definition JSON
```

## Project Layout

```
flowforge/
├── docker-compose.yml          # Full 10-service stack
├── scripts/
│   ├── init-localstack.sh      # Creates S3 buckets + DynamoDB table
│   └── seed-data.sh            # Uploads test data to S3
├── test-data/
│   ├── sales.csv               # 20 sample sales records
│   └── products.json           # 10 sample product objects
├── backend/                    # Spring Boot 3.2 / Java 17
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/flowforge/
│       ├── config/             # RabbitMQ, Kafka, Redis, S3, WebSocket, CORS
│       ├── controller/         # PipelineController, ExecutionController, AIController
│       ├── model/              # Pipeline, PipelineDefinition, TaskMessage, CompletionEvent
│       ├── rabbitmq/           # TaskPublisher, DLQHandler
│       ├── repository/         # JPA repos (Pipeline, NodeType)
│       ├── service/            # PipelineService, ExecutionOrchestrator, StatusService, AIService, AuditService
│       └── worker/             # BaseWorker + 8 node executors
├── flink-jobs/                 # Standalone Flink 1.18 JAR jobs
│   └── src/main/java/com/flowforge/flink/
│       ├── FilterJob.java
│       └── AggregatorJob.java
└── frontend/                   # React 18 + React Flow 11 + Vite
    └── src/
        ├── api/pipelineApi.js
        ├── hooks/useExecutionStatus.js
        └── components/
            ├── AI/             # AIPipelineChat
            ├── Canvas/         # PipelineCanvas, NodePalette, EdgeValidator
            ├── Config/         # NodeConfigPanel
            ├── Dashboard/      # ExecutionMonitor, NodeStatusBadge
            └── Nodes/          # BaseNode + 8 typed node wrappers
```

## Development (without Docker)

```bash
# Backend (requires Postgres, RabbitMQ, Kafka, Redis running locally)
cd backend
mvn spring-boot:run

# Frontend
cd frontend
npm install
npm run dev       # http://localhost:5173 (proxies /api to :8080)
```

Set env vars to override service addresses (see `application.yml` for the full list).

## Key Design Decisions

- **Two-plane architecture**: RabbitMQ handles orchestration/control; Kafka handles record streaming. This keeps orchestration logic separate from data throughput.
- **Per-edge Kafka topics**: Each pipeline edge gets its own topic (`flowforge.{execId}.{src}_to_{tgt}`), enabling parallel execution of independent branches.
- **Synchronous drain for PoC**: Workers use a `KafkaConsumer.poll()` loop (2 consecutive empty polls = done) rather than streaming Flink jobs for most node types, keeping the execution model simple.
- **Redis for live state**: Execution and node statuses are stored in Redis with a 1-hour TTL; the frontend polls every 2 seconds while a pipeline is running.
- **JSONB pipeline definitions**: The pipeline `definition` (nodes + edges) is stored as JSONB in Postgres, enabling schema-free evolution.
- **Ollama for AI generation**: The AI service calls a local Ollama instance (llama3 model) and caches responses in Redis keyed by SHA-256(prompt). Falls back to a demo pipeline on any error.
