# MetroFlow API

A TypeScript/Node.js REST API providing real-time and scheduled transit data for New York's Metropolitan Transportation Authority (MTA) systems.

## Supported Transit Systems

- **NYC Subway (NYCT)** - All numbered and lettered lines
- **Long Island Rail Road (LIRR)** - All 12 branches
- **Metro-North Railroad (MNR)** - Hudson, Harlem, New Haven lines and branches

## Features

- **Real-time departures** with delay calculations and track/platform info
- **Service alerts** with filtering by line, station, and active status
- **Station search** across all systems with ADA accessibility data
- **Scheduled departures** from GTFS static data
- **Intelligent caching** with system-specific TTLs
- **Analytics tracking** for usage monitoring
- **Health endpoints** for operational monitoring

## Quick Start

### Prerequisites

- Node.js 18+
- pnpm (recommended) or npm

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/metroflow-api.git
cd metroflow-api

# Install dependencies
pnpm install

# Copy environment template and configure
cp .env.example .env

# Build the project
pnpm run build

# Start the server
pnpm start
```

### Development Mode

```bash
pnpm run dev
```

## API Endpoints

All endpoints are prefixed with `/api/v1`

### Stations

```
GET /api/v1/stations?q={query}&system={SUBWAY|LIRR|MNR}
```

Search for stations by name or stop ID. Optionally filter by transit system.

**Response:**
```json
[
  {
    "stopId": "101",
    "stopName": "Van Cortlandt Park - 242 St",
    "stopLat": 40.889248,
    "stopLon": -73.898583,
    "lines": ["1"],
    "borough": "Bronx",
    "ada": true,
    "system": "SUBWAY"
  }
]
```

### Departures

```
GET /api/v1/departures/:stationId?limitMinutes={n}&source={realtime|scheduled}
```

Get real-time and/or scheduled departures for a station.

**Parameters:**
- `stationId` - Station/stop ID
- `limitMinutes` - Limit results to next N minutes (optional)
- `source` - Filter by `realtime`, `scheduled`, or both (optional)

**Response:**
```json
[
  {
    "tripId": "123456_1..N03R",
    "routeId": "1",
    "stopId": "101N",
    "arrivalTime": "2024-01-15T14:30:00-05:00",
    "departureTime": "2024-01-15T14:30:30-05:00",
    "delay": 120,
    "destination": "South Ferry",
    "direction": "S",
    "track": null,
    "source": "realtime"
  }
]
```

### Alerts

```
GET /api/v1/alerts?lines={lines}&activeNow={bool}&stationId={id}&includeLabels={bool}
```

Get service alerts across all MTA systems.

**Parameters:**
- `lines` - Comma-separated route IDs (e.g., `SUBWAY-1,LIRR-3`)
- `activeNow` - Filter to currently active alerts only
- `stationId` - Filter alerts affecting a specific station
- `includeLabels` - Include human-readable line/station names

**Response:**
```json
[
  {
    "id": "lmm:alert:123",
    "headerText": "1 Train Service Change",
    "descriptionText": "Southbound 1 trains are running local...",
    "affectedLines": ["SUBWAY-1"],
    "affectedStations": ["101", "102"],
    "activePeriods": [
      {
        "start": "2024-01-15T00:00:00-05:00",
        "end": "2024-01-16T04:00:00-05:00"
      }
    ]
  }
]
```

### Health

```
GET /api/health
```

Basic health check (public).

```
GET /api/health/detailed?api_key={key}
```

Detailed health information including memory usage, cache stats, and uptime. Requires API key via query parameter or `X-API-Key` header.

### Analytics

```
GET /api/analytics/stations
GET /api/analytics/usage
```

Query analytics data for station popularity and API usage patterns.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ENV` | Environment (`development`/`production`) | - |
| `PORT` | Server port | `3000` |
| `LOG_LEVEL` | Logging level (`debug`, `info`, `warn`, `error`) | `info` |
| `MTA_API_BASE` | MTA GTFS-RT API base URL | - |
| `GTFS_STATIC_URL_NYCT` | NYC Subway GTFS static ZIP URL | - |
| `GTFS_STATIC_URL_LIRR` | LIRR GTFS static ZIP URL | - |
| `GTFS_STATIC_URL_MNR` | MNR GTFS static ZIP URL | - |
| `CACHE_TTL_SECONDS` | Default cache TTL (seconds) | `60` |
| `CACHE_TTL_SUBWAY` | Subway cache TTL | `60` |
| `CACHE_TTL_LIRR` | LIRR cache TTL | `120` |
| `CACHE_TTL_MNR` | MNR cache TTL | `120` |
| `CACHE_TTL_ALERTS` | Alerts cache TTL | `300` |
| `STATIC_REFRESH_SCHEDULE` | Cron schedule for GTFS refresh | `0 4 * * *` |
| `STATIC_REFRESH_TIMEZONE` | Timezone for cron schedule | `America/New_York` |
| `HEALTH_CHECK_API_KEY` | API key for detailed health endpoint | - |

## Docker Deployment

### Build and Push

```bash
# Build the image
docker buildx build -t yourregistry/metroflow-api:1.0.0 --push .
```

### Run with Docker Compose

```bash
docker-compose up -d
```

The compose configuration includes:
- Automatic restart on failure
- Named volumes for data persistence
- Health checks
- Log rotation (50MB max, 10 files)
- Non-root user for security

## Project Structure

```
src/
├── server.ts              # Application entry point
├── routes/
│   ├── index.ts           # Route registration, health/analytics endpoints
│   └── mtaRoutes.ts       # MTA API endpoints
├── services/
│   ├── mtaService.ts      # Real-time departures and station queries
│   ├── alertService.ts    # Service alert processing
│   ├── staticDataService.ts   # GTFS static data management
│   ├── cacheService.ts    # In-memory caching
│   ├── analyticsService.ts    # Usage analytics (SQLite)
│   ├── calendarService.ts # Service calendar logic
│   └── geoService.ts      # Geographic lookups
├── config/
│   └── systemConfig.ts    # System-specific configurations
├── utils/
│   ├── logger.ts          # Winston logger configuration
│   ├── gtfsFeedParser.ts  # GTFS-RT protobuf parsing
│   └── csvParser.ts       # GTFS CSV parsing
├── types/
│   └── index.ts           # TypeScript interfaces
├── middleware/
│   └── apiTracker.ts      # Analytics middleware
└── tasks/
    └── refreshStaticData.ts   # Scheduled GTFS refresh task
```

## Data Sources

### Real-Time Data
- MTA GTFS-RT feeds (Protocol Buffer format)
- Updated approximately every 30 seconds
- Includes MTA-specific extensions for track info, wheelchair status

### Static Data
- GTFS ZIP archives from MTA
- Contains schedules, stops, routes, and service calendars
- Auto-refreshed daily (configurable)

## Scripts

| Command | Description |
|---------|-------------|
| `pnpm run build` | Compile TypeScript |
| `pnpm run dev` | Development mode with hot reload |
| `pnpm start` | Production mode |
| `pnpm run task:refresh-static` | Manually refresh GTFS static data |
| `pnpm run db:reset:analytics` | Reset analytics database |

## Tech Stack

- **Runtime:** Node.js 18+
- **Language:** TypeScript (strict mode)
- **Framework:** Express.js 5
- **Caching:** node-cache (in-memory)
- **Database:** SQLite (better-sqlite3) for analytics
- **Logging:** Winston
- **Scheduling:** node-cron
- **Protobuf:** protobufjs for GTFS-RT parsing

## Security

- Rate limiting (1000 requests per 15 minutes per IP)
- Security headers (X-Frame-Options, HSTS, etc.)
- Non-root Docker user
- API key protection for sensitive endpoints

## License

MIT
