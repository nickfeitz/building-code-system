# Building Code Intelligence System - Startup Guide

## Prerequisites

1. Docker and Docker Compose installed
2. PostgreSQL container running in `postgres-stack` project with:
   - Container name: `postgres`
   - Port: 5432
   - User: `postgres`
   - Password: `F31t53r1`
   - Database: `appdb`
   - Network: `postgres-stack_default`
   - Extensions: pgvector, pg_trgm (already installed)

## Quick Start

### 1. Configure Environment

Update the `.env` file with your settings:
- `CLAUDE_API_KEY`: Replace `sk-ant-CHANGEME` with your actual Claude API key

```bash
# View current configuration
cat .env

# Update API key (optional - system works without it but chat features unavailable)
sed -i 's/sk-ant-CHANGEME/your-actual-key-here/' .env
```

### 2. Initialize Database Schema

Before starting the services, initialize the database schema:

```bash
# Connect to postgres container and run schema
docker exec -i postgres psql -U postgres -d appdb < backend/schema.sql

# Seed initial data (optional)
# Build the backend image first to have Python available
docker build -t building-code-backend backend/
docker run --rm --network postgres-stack_default \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=F31t53r1 \
  -e POSTGRES_DB=appdb \
  building-code-backend python seed.py
```

### 3. Start Services

```bash
# From the building-code-system directory
docker compose up -d

# View logs
docker compose logs -f

# Check service status
docker compose ps
```

### 4. Verify Services

```bash
# Check health endpoints
curl http://localhost:8010/api/health
curl http://localhost:8011/health
curl http://localhost:3010/

# Expected responses:
# Backend: {"status":"healthy","database":"ok","embedding_service":"ok","claude_api":"ok|not configured"}
# Embedding: {"status":"healthy","model":"intfloat/e5-large-v2","model_dimension":1024}
# Frontend: HTML page with "Building Code Intelligence System"
```

## Service Access

- **Frontend UI**: http://localhost:3010
- **Backend API**: http://localhost:8010
- **Embedding Service**: http://localhost:8011

## API Endpoints

### Health & Status
- `GET /api/health` - System health check

### Chat
- `POST /api/chat` - Chat with Claude (streaming)

### Search
- `GET /api/sections/search?q=&code_book_id=&chapter=&has_amendment=` - Search sections
- `GET /api/sections/{id}` - Get section details
- `GET /api/sections/{id}/references` - Get section references

### Import
- `POST /api/import/upload` - Upload PDF
- `POST /api/import/trigger/{source_id}` - Trigger import
- `GET /api/import/status` - Check import status

### Content Quarantine
- `GET /api/quarantine` - List quarantined items
- `POST /api/quarantine/{id}/approve` - Approve item
- `POST /api/quarantine/{id}/reject` - Reject item

## Architecture

### Services

**embedding-service** (Port 8011)
- Python FastAPI application
- Uses sentence-transformers E5-large-v2 model
- Generates vector embeddings for semantic search
- Model cached on first load

**backend** (Port 8010)
- Python FastAPI application
- Connects to PostgreSQL
- Integrates with Claude API
- Implements content validation pipeline (4 layers)
- APScheduler for crawl scheduling
- Async connection pooling with asyncpg

**frontend** (Port 3010)
- Nginx web server
- Static HTML/CSS/JS serving
- Proxies `/api/*` requests to backend
- Health check via HTTP

### Networks

- `postgres-stack_default` - External network connecting to postgres-stack project
- `building-code-network` - Internal network for inter-service communication

## Configuration

### Environment Variables

```
COMPOSE_PROJECT_NAME=building-code-system
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=F31t53r1
POSTGRES_DB=appdb
CLAUDE_API_KEY=sk-ant-CHANGEME
EMBEDDING_MODEL=intfloat/e5-large-v2
EMBEDDING_PORT=8011
BACKEND_PORT=8010
FRONTEND_PORT=3010
```

## Database Schema

The system includes comprehensive database schema with:
- Code cycles and cycles for different years
- Publishing organizations (ICC, NFPA, IAPMO, CBSC)
- Code books (Title 24 Parts 1-12)
- Code sections with full-text and vector search indexes
- Section version history and audit trail
- External standards references
- Code references (internal and external)
- Topics and section tagging
- Import sources and logs
- Content quarantine for validation pipeline
- Chat sessions and messages
- User annotations

### Key Tables

- `code_cycles` - Year-based code versions
- `code_books` - Organized code sections (Title 24 Parts)
- `code_sections` - Individual sections with embeddings
- `code_references` - Internal and external standard references
- `external_standards` - ASTM, NFPA, and other standards
- `content_quarantine` - 4-layer validation pipeline results
- `chat_sessions` - User chat history
- `import_logs` - Tracking of data imports

## Troubleshooting

### Service won't start

```bash
# Check logs
docker compose logs embedding-service
docker compose logs backend
docker compose logs frontend

# Verify PostgreSQL connection
docker exec postgres psql -U postgres -d appdb -c "SELECT 1"

# Verify network
docker network ls
docker network inspect postgres-stack_default
```

### Database connection issues

```bash
# Test connection from backend container
docker exec backend python -c "import asyncpg; print('asyncpg OK')"

# Check PostgreSQL extensions
docker exec postgres psql -U postgres -d appdb -c "CREATE EXTENSION IF NOT EXISTS pgvector"
docker exec postgres psql -U postgres -d appdb -c "CREATE EXTENSION IF NOT EXISTS pg_trgm"
```

### Embedding service errors

```bash
# Check if model is loading
docker logs embedding-service | grep -i "loading\|error"

# Verify service responds
curl -v http://localhost:8011/health
```

## Scaling & Production

For production deployment:

1. Update `.env` with strong passwords and valid API keys
2. Configure proper PostgreSQL backups
3. Use environment-specific Dockerfiles
4. Implement rate limiting and API authentication
5. Set up monitoring and alerting
6. Use secrets management (AWS Secrets Manager, Vault, etc.)
7. Configure log aggregation
8. Set resource limits in docker-compose.yml

## Stopping Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes (careful - deletes data!)
docker compose down -v

# View stopped containers
docker compose ps -a
```

## Next Steps

1. Update frontend with full React/Vue.js application
2. Implement PDF upload and parsing
3. Set up web crawling pipeline for code updates
4. Configure Claude API integration for smart chat
5. Implement user authentication
6. Add analytics and usage tracking
7. Deploy to production infrastructure
