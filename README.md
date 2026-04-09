# Building Code Intelligence System - Docker Infrastructure

Complete Docker infrastructure for the Building Code Intelligence System that connects to an existing PostgreSQL instance.

## Project Structure

```
building-code-system/
├── docker-compose.yml           # Service orchestration
├── .env                         # Environment configuration
├── STARTUP.md                   # Detailed startup guide
├── README.md                    # This file
├── embedding-service/           # Embedding generation service
│   ├── Dockerfile
│   ├── main.py                  # FastAPI app with sentence-transformers
│   └── requirements.txt
├── backend/                     # Main API service
│   ├── Dockerfile
│   ├── main.py                  # FastAPI with all endpoints
│   ├── requirements.txt
│   ├── schema.sql               # Complete database schema
│   └── seed.py                  # Data seeding script
└── frontend/                    # Nginx web server
    ├── Dockerfile
    ├── nginx.conf               # Reverse proxy configuration
    └── index.html               # Placeholder UI
```

## Quick Start

### 1. Prerequisites

- Docker and Docker Compose
- Existing PostgreSQL running in `postgres-stack` project
  - Host: `postgres` (container name)
  - Port: 5432
  - User: `postgres`
  - Password: `F31t53r1`
  - Database: `building_code` — **dedicated to this project**; do not share with NocoDB, n8n, or reconciliation-platform
  - Extensions: pgvector, pg_trgm (pre-installed)

### 2. Initialize Database

```bash
# Create the dedicated database (first time only)
docker exec postgres psql -U postgres -c "CREATE DATABASE building_code OWNER postgres;"
docker exec postgres psql -U postgres -d building_code -c "CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS pg_trgm;"

# Apply schema
docker exec -i postgres psql -U postgres -d building_code < backend/schema.sql

# Seed initial data (optional)
docker build -t bcs-backend backend/
docker run --rm --network postgres-stack_default \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=F31t53r1 \
  -e POSTGRES_DB=building_code \
  bcs-backend python seed.py
```

### 3. Update Configuration

```bash
# Edit .env and set CLAUDE_API_KEY
nano .env
```

### 4. Start Services

```bash
docker compose up -d

# View logs
docker compose logs -f

# Check status
docker compose ps
```

### 5. Access Services

- **Frontend**: http://localhost:3010
- **Backend API**: http://localhost:8010/api/health
- **Embedding Service**: http://localhost:8011/health

## Services Overview

### Embedding Service (Port 8011)

**Purpose**: Generate vector embeddings for semantic search

**Stack**: Python 3.11 + FastAPI + sentence-transformers E5-large-v2

**Endpoints**:
- `GET /health` - Health check
- `POST /embed` - Generate embedding from text

**Features**:
- Caches E5-large-v2 model on first load
- Returns 1024-dimensional vectors
- ARM64 compatible (CPU-based, no CUDA)
- Async request handling

**Environment Variables**:
- `EMBEDDING_MODEL=intfloat/e5-large-v2`
- `EMBEDDING_PORT=8011`

### Backend Service (Port 8010)

**Purpose**: Main API server with database integration and AI features

**Stack**: Python 3.11 + FastAPI + asyncpg + Claude API + APScheduler

**Key Endpoints**:

**Health & Admin**
- `GET /api/health` - System health check

**Chat & Intelligence**
- `POST /api/chat` - Chat with Claude (streaming response)

**Search & Browse**
- `GET /api/sections/search` - Search code sections
- `GET /api/sections/{id}` - Get section details
- `GET /api/sections/{id}/references` - Get references

**Data Import**
- `POST /api/import/upload` - Upload PDF
- `POST /api/import/trigger/{source_id}` - Trigger import
- `GET /api/import/status` - Check import status

**Content Quarantine** (4-layer validation)
- `GET /api/quarantine` - List flagged content
- `POST /api/quarantine/{id}/approve` - Approve item
- `POST /api/quarantine/{id}/reject` - Reject item

**Features**:
- Full AsyncIO with connection pooling (asyncpg)
- Claude API with 3-retry exponential backoff
- APScheduler for scheduled crawls
- 4-layer content validation pipeline
- Comprehensive error handling
- All response shapes properly defined

**Environment Variables**:
- `POSTGRES_HOST=postgres`
- `POSTGRES_PORT=5432`
- `POSTGRES_USER=postgres`
- `POSTGRES_PASSWORD=F31t53r1`
- `POSTGRES_DB=building_code`
- `CLAUDE_API_KEY=sk-ant-CHANGEME`
- `EMBEDDING_SERVICE_URL=http://embedding-service:8011`
- `BACKEND_PORT=8010`

### Frontend Service (Port 3010)

**Purpose**: Serve web UI and proxy API requests

**Stack**: Nginx Alpine with static HTML/CSS/JS

**Features**:
- Serves static files with caching
- Proxies `/api/*` to backend service
- Security headers configured
- Gzip compression enabled
- Health check support

**Configuration**:
- Reverse proxy to backend:8010
- Static asset caching (1 year)
- SPA support with index.html fallback

## Database Schema

### Core Tables

**Code Organization**
- `code_cycles` - Year-based code versions (2022, 2025 California cycles)
- `publishing_orgs` - Organizations (ICC, NFPA, IAPMO, CBSC)
- `code_books` - Title 24 Parts 1-12

**Content**
- `code_sections` - Individual sections with embedding vectors
- `code_section_versions` - Version history and audit trail
- `external_standards` - ASTM, NFPA, and other standards
- `code_references` - Cross-references (internal and external)

**Organization**
- `topics` - Tagging system
- `section_topics` - Section-to-topic mapping

**Operations**
- `import_sources` - Data source configurations
- `import_logs` - Import execution history
- `content_quarantine` - Failed validation items (4-layer pipeline)

**Intelligence**
- `chat_sessions` - User chat history
- `chat_messages` - Individual messages
- `user_annotations` - User comments and notes

### Indexes

- **Vector search**: HNSW index on `code_sections.embedding`
- **Full-text search**: GIN indexes on title/content
- **Foreign keys**: B-tree indexes for all relationships
- **Composite**: Common query patterns optimized
- **Text search**: PostgreSQL tsvector index for full-text

## Configuration

### Environment File (.env)

```env
COMPOSE_PROJECT_NAME=building-code-system
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=F31t53r1
POSTGRES_DB=building_code
CLAUDE_API_KEY=sk-ant-CHANGEME
EMBEDDING_MODEL=intfloat/e5-large-v2
EMBEDDING_PORT=8011
BACKEND_PORT=8010
FRONTEND_PORT=3010
```

### Docker Compose Configuration

**Networks**:
- `postgres-stack_default` - External network connecting to postgres-stack project
- `building-code-network` - Internal network for inter-service communication

**Health Checks**: All services include health checks with:
- 10-second interval
- 5-second timeout
- 3 retry attempts
- Appropriate start periods

**Dependencies**:
- `backend` depends on `embedding-service` (service_healthy)
- `frontend` depends on `backend`

## Data Seeding

The `seed.py` script initializes the database with:

**Organizations**: ICC, NFPA, IAPMO, CBSC

**Code Cycles**: 2022 and 2025 California Building Code

**Code Books**: All 12 parts of Title 24
- Part 1: Administration
- Part 2: Building Planning
- Part 3: Fire and Life Safety
- Part 4: Accessibility
- Part 5: General Building Safety Provisions
- Part 6: Building Elements and Materials
- Part 7: Fire-Resistance-Rated Construction
- Part 8: Interior Finishes
- Part 9: Structural Design
- Part 10: Means of Egress
- Part 11: Accessibility
- Part 12: Interior Environment

**External Standards**: 8 key standards (ASTM E119, NFPA 13/72/101, etc.)

**Topics**: 20 building code topic tags

**Sample Sections**: 4 sample sections for Part 2 with version history

## Features Implemented

### Embedding Service
- Load sentence-transformers model on startup
- Cache model in memory
- Generate 1024-dimensional vectors
- Health endpoint with model info

### Backend API
- Health check with database/embedding/Claude verification
- Streaming chat with Claude API
- Section search with filters
- Section detail retrieval
- Reference browsing (internal and external)
- PDF upload handling
- Import triggering and status checking
- Content quarantine management
- 3-retry exponential backoff for Claude API
- APScheduler setup for crawl scheduling
- Full Pydantic model definitions for all responses
- Proper error handling and HTTP status codes

### Frontend
- Responsive HTML placeholder
- System status indicators
- Feature list
- Nginx reverse proxy
- Security headers
- Static asset caching
- Gzip compression

### Database
- Complete schema with 14 tables
- All relationships and constraints defined
- Strategic indexes (vector, full-text, composite)
- Version history tracking
- Validation quarantine pipeline
- Chat session management
- Comprehensive audit trail

## Deployment

### Local Development

```bash
docker compose up -d
docker compose logs -f
docker compose ps
```

### Testing

```bash
# Test health endpoints
curl http://localhost:8010/api/health
curl http://localhost:8011/health
curl http://localhost:3010/

# Test search (after seeding)
curl "http://localhost:8010/api/sections/search?q=administration"

# Test embedding
curl -X POST http://localhost:8011/embed \
  -H "Content-Type: application/json" \
  -d '{"text":"fire safety requirements"}'
```

### Production Considerations

1. Use strong passwords and secret management
2. Set proper resource limits
3. Enable persistent volumes for PostgreSQL
4. Configure log aggregation
5. Implement API rate limiting
6. Add authentication/authorization
7. Use HTTPS/TLS
8. Monitor service health
9. Set up automated backups
10. Configure horizontal scaling

## Troubleshooting

### Services won't start

```bash
# Check logs
docker compose logs embedding-service
docker compose logs backend
docker compose logs frontend

# Verify network connectivity
docker network inspect postgres-stack_default
```

### Database connection errors

```bash
# Test PostgreSQL
docker exec postgres psql -U postgres -d building_code -c "SELECT 1"

# Verify extensions
docker exec postgres psql -U postgres -d building_code -c "\dx"

# Check schema creation
docker exec postgres psql -U postgres -d building_code -c "\dt"
```

### Embedding service not responding

```bash
# Check logs
docker compose logs embedding-service

# Verify endpoint
curl -v http://localhost:8011/health

# Check model download (may take time on first run)
docker logs embedding-service | grep -i "loading\|model"
```

## File Manifest

| File | Purpose | Lines |
|------|---------|-------|
| docker-compose.yml | Service orchestration | 77 |
| .env | Environment configuration | 11 |
| embedding-service/Dockerfile | Container image | 23 |
| embedding-service/main.py | FastAPI app | 80 |
| embedding-service/requirements.txt | Python dependencies | 6 |
| backend/Dockerfile | Container image | 24 |
| backend/main.py | FastAPI app with endpoints | 527 |
| backend/requirements.txt | Python dependencies | 12 |
| backend/schema.sql | Database schema | 217 |
| backend/seed.py | Initial data | 225 |
| frontend/Dockerfile | Container image | 14 |
| frontend/nginx.conf | Reverse proxy config | 48 |
| frontend/index.html | Placeholder UI | 254 |
| STARTUP.md | Detailed startup guide | 254 |
| README.md | This file | ~500 |

## Next Steps

1. **Frontend Development**: Build full React/Vue.js UI
2. **PDF Processing**: Implement PDF parsing and extraction
3. **Web Crawling**: Set up automated code updates
4. **Authentication**: Add user auth and authorization
5. **Analytics**: Track usage and performance
6. **Integration**: Connect to external code sources
7. **Testing**: Add comprehensive test suites
8. **Monitoring**: Set up alerts and dashboards

## Architecture Notes

### Async Design
All Python services use async/await for:
- Database connections (asyncpg)
- HTTP requests (httpx)
- API endpoints (FastAPI)
- Task scheduling (APScheduler)

### Error Handling
- Claude API: 3 retries with exponential backoff (2s, 4s, 8s)
- Database: Connection pool with automatic reconnection
- Health checks: Graceful degradation with status reporting

### Validation Pipeline
4-layer content validation:
1. Format validation (PDF/text extraction)
2. Structure validation (section hierarchy)
3. Content validation (required fields)
4. Compliance validation (against standards)

## Support

For detailed startup instructions, see `STARTUP.md`.

For issues or questions:
1. Check docker compose logs
2. Verify PostgreSQL connectivity
3. Ensure all environment variables are set
4. Confirm network connectivity between services
5. Review health check endpoints
