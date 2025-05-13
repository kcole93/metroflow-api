# MetroFlow API Production Deployment Guide

This document outlines best practices and requirements for deploying the MetroFlow API to production environments.

## Prerequisites

- Docker and Docker Compose installed on the host machine
- Sufficient disk space for GTFS static data (at least 1GB)
- A production-ready environment with:
  - Adequate memory (minimum 2GB RAM)
  - Proper network connectivity
  - Firewall access for outbound connections to MTA APIs

## Environment Variables

The application requires the following environment variables to be set:

### Core Configuration
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `NODE_ENV` | Environment mode | `development` | Yes - set to `production` |
| `PORT` | Port to run the API on | `3000` | No |
| `LOG_LEVEL` | Logging verbosity | `info` | No |
| `MTA_API_BASE` | Base URL for MTA API | See `.env` | Yes |
| `HEALTH_CHECK_API_KEY` | Secret key for detailed health check access | None | No - But recommended |

### Static Data URLs
| Variable | Description | Required |
|----------|-------------|----------|
| `GTFS_STATIC_URL_NYCT` | URL to NYCT subway static data | Yes |
| `GTFS_STATIC_URL_LIRR` | URL to LIRR static data | Yes |
| `GTFS_STATIC_URL_MNR` | URL to MNR static data | Yes |

### Caching Configuration
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `CACHE_TTL_SECONDS` | Default cache lifetime in seconds | `60` | No |
| `CACHE_TTL_SUBWAY` | Cache lifetime for subway data | `60` | No |
| `CACHE_TTL_LIRR` | Cache lifetime for LIRR data | `120` | No |
| `CACHE_TTL_MNR` | Cache lifetime for MNR data | `120` | No |
| `CACHE_TTL_ALERTS` | Cache lifetime for alerts | `300` | No |
| `CACHE_MAX_KEYS` | Maximum cache keys | `1000` | No |

### GTFS Refresh Settings
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `STATIC_REFRESH_SCHEDULE` | Cron schedule for static data refresh | `0 4 * * *` | No |
| `STATIC_REFRESH_TIMEZONE` | Timezone for refresh schedule | `America/New_York` | No |

## Deployment Steps

### 1. Prepare Environment File

Create a `.env` file in the project root with the required variables:

```
NODE_ENV=production
PORT=3000
LOG_LEVEL=info
MTA_API_BASE=https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds
PROTO_BASE_PATH=com/google/transit/realtime/gtfs-realtime.proto
PROTO_NYCT_PATH=com/google/transit/realtime/gtfs-realtime-NYCT.proto
PROTO_MTARR_PATH=com/google/transit/realtime/gtfs-realtime-MTARR.proto
GTFS_STATIC_URL_NYCT=https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip
GTFS_STATIC_URL_LIRR=https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip
GTFS_STATIC_URL_MNR=https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip
STATIC_REFRESH_SCHEDULE="0 4 * * *"
STATIC_REFRESH_TIMEZONE="America/New_York"
CACHE_TTL_SECONDS=120
HEALTH_CHECK_API_KEY=your_secure_api_key_here
```

### 2. Deploy with Docker Compose

```bash
# Build and start the containers in detached mode
docker-compose up -d

# Check the container logs
docker-compose logs -f metroflow-api

# Verify the container is running correctly
curl http://localhost:3000/api/health
curl http://localhost:3000/api/health/detailed?api_key=your_secure_api_key_here
```

### 3. Monitor Container Health

The container includes health check endpoints which can be monitored:

```bash
# Check container status including health
docker ps -a

# Check basic health (publicly accessible)
curl http://localhost:3000/api/health

# Check detailed health (requires API key)
curl -H "X-API-Key: your_secure_api_key_here" http://localhost:3000/api/health/detailed
```

## Scaling Considerations

### Horizontal Scaling

The MetroFlow API can be deployed behind a load balancer with multiple instances for horizontal scaling. Each instance should:

1. Have its own data volume for analytics data
2. Run the static data refresh task (the task implementation prevents multiple concurrent runs)

### Memory Optimization

If memory usage is a concern, consider:

1. Adjusting the `CACHE_TTL_*` values to reduce cache lifetime
2. Setting `CACHE_MAX_KEYS` to limit memory usage by cache
3. Monitoring memory usage through the `/api/health` endpoint

### Security Considerations

### Network Security

1. The API server should be behind a reverse proxy or load balancer
2. TLS/SSL should be terminated at the proxy level
3. API rate limiting is implemented but additional WAF rules are recommended

### Data Security

1. The API doesn't store sensitive user data
2. Analytics data is stored in SQLite database files that should be properly backed up

### Health Check Security

The API provides two health check endpoints:

1. `/api/health` - Basic health status (publicly accessible)
2. `/api/health/detailed` - Detailed system information (secured)

The detailed health check endpoint is protected and can be accessed:
- Using the `HEALTH_CHECK_API_KEY` via header `X-API-Key` or query parameter `api_key`
- From localhost only, if no API key is configured

## Backup Procedures

### Volume Backups

The Docker Compose configuration creates three named volumes that should be backed up regularly:

- `metroflow-data`: Contains analytics database
- `metroflow-logs`: Contains application logs
- `metroflow-temp`: Contains temporary download files

Backup example:

```bash
# Stop the container
docker-compose stop

# Backup the volumes
docker run --rm -v metroflow-data:/source -v /path/to/backup:/dest alpine tar -czf /dest/metroflow-data-$(date +%Y%m%d).tar.gz -C /source .
docker run --rm -v metroflow-logs:/source -v /path/to/backup:/dest alpine tar -czf /dest/metroflow-logs-$(date +%Y%m%d).tar.gz -C /source .

# Restart the container
docker-compose start
```

## Monitoring

### Key Metrics to Monitor

1. **System Health**: CPU, memory, disk usage on the host
2. **API Health**: Response times, error rates, request volume
3. **Cache Efficiency**: Cache hit/miss rates (via `/api/health/detailed` endpoint)
4. **Static Data Refresh**: Success/failure of scheduled GTFS data updates

### Log Management

The application logs to stdout/stderr and to the `/app/logs` directory. In production, consider:

1. Implementing a log aggregation solution
2. Setting up log rotation to prevent disk space issues
3. Setting up alerts for ERROR level log messages

## Troubleshooting

### Common Issues

#### Static Data Loading Failures

If the application fails to load static data:

1. Check if GTFS_STATIC_URL_* environment variables are correctly set
2. Verify network connectivity to the static data sources
3. Check for sufficient disk space

#### Performance Issues

If the API is responding slowly:

1. Check system resource usage
2. Monitor the number of concurrent requests
3. Adjust caching parameters
4. Consider scaling the deployment horizontally

#### Database Errors

If you see SQLite-related errors:

1. Check disk space and permissions
2. Verify the volumes are properly mounted
3. Consider backing up and recreating the analytics database:
   ```bash
   docker-compose exec metroflow-api npm run db:reset:analytics
   ```

## Upgrade Procedures

To upgrade the application:

1. Pull the latest code changes
2. Build a new Docker image
3. Test the new image in a staging environment
4. Deploy to production:
   ```bash
   docker-compose down
   docker-compose up -d --build
   ```

## Version Information

It's important to maintain a record of the deployed version. The Docker image is tagged with version information in the Dockerfile:

```
LABEL version="1.0.0"
```

Check the current running version with:

```bash
docker inspect --format '{{ index .Config.Labels "version" }}' metroflow-api
```
