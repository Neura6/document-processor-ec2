# Dockerized PDF Processor

## Quick Start

### 1. Setup Environment
```bash
# Copy environment template
cp .env.example .env

# Edit with your AWS credentials
nano .env
```

### 2. Single Command Deployment
```bash
docker-compose up -d
```

### 3. Verify Deployment
```bash
# Check all services
docker-compose ps

# View logs
docker-compose logs -f pdf-processor

# Access dashboards
# Grafana: http://localhost:3000 (admin/admin123)
# Prometheus: http://localhost:9090
# Metrics: http://localhost:8000/metrics
```

## Services Running

| Service | Port | Description |
|---------|------|-------------|
| **pdf-processor** | 8000 | Main SQS worker + metrics |
| **prometheus** | 9090 | Metrics collection |
| **grafana** | 3000 | Monitoring dashboards |

## Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Scale workers
docker-compose up -d --scale pdf-processor=3

# Rebuild after changes
docker-compose build --no-cache
```

## Environment Variables

Required in `.env` file:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `SQS_QUEUE_URL`
- `AWS_REGION` (optional, defaults to us-east-1)

## Health Checks

All services include health checks:
- **pdf-processor**: http://localhost:8000/metrics
- **prometheus**: http://localhost:9090/-/healthy
- **grafana**: http://localhost:3000/api/health
