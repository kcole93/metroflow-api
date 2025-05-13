# Dockerfile

# ---- Builder Stage ----
FROM node:18-alpine AS builder

# Add metadata labels
LABEL maintainer="MetroFlow API Team"
LABEL version="1.0.0"
LABEL description="MetroFlow API for MTA, LIRR, and MNR transit data"

# Set build arguments with defaults
ARG NODE_ENV=production
ARG PORT=3000

# Set environment variables
ENV NODE_ENV=${NODE_ENV}
ENV PORT=${PORT}

# Use non-root user for better security
RUN addgroup -g 1001 -S nodejs && adduser -S apiuser -G nodejs -u 1001

# Create app directory and set ownership
WORKDIR /app

# Install pnpm globally within the builder
RUN npm install -g pnpm@latest

# Copy package files
COPY --chown=apiuser:nodejs package.json pnpm-lock.yaml ./

# Install ALL dependencies (including dev for build)
# Using --frozen-lockfile ensures exact versions from lockfile
RUN pnpm install --frozen-lockfile

# Copy the rest of the application code
COPY --chown=apiuser:nodejs . .

# Create necessary directories
RUN mkdir -p dist/assets/gtfs-static dist/assets/geodata logs data temp-downloads && \
    chown -R apiuser:nodejs dist logs data temp-downloads

# Build TypeScript to JavaScript in /app/dist
RUN pnpm run build

# Prune dev dependencies
RUN pnpm prune --prod


# ---- Final Stage ----
FROM node:18-alpine AS final

# Add metadata labels
LABEL maintainer="MetroFlow API Team"
LABEL version="1.0.0" 
LABEL description="MetroFlow API for MTA, LIRR, and MNR transit data"

# Set environment variables
ENV NODE_ENV=production
ENV PORT=3000

# Install system dependencies
RUN apk --no-cache add dumb-init tzdata

# Use non-root user for better security
RUN addgroup -g 1001 -S nodejs && adduser -S apiuser -G nodejs -u 1001

WORKDIR /app

# Copy production node_modules from builder
COPY --from=builder --chown=apiuser:nodejs /app/node_modules ./node_modules

# Copy built application from builder
COPY --from=builder --chown=apiuser:nodejs /app/dist ./dist

# Copy assets needed at runtime (proto files, geojson)
COPY --from=builder --chown=apiuser:nodejs /app/src/assets ./dist/assets
COPY --from=builder --chown=apiuser:nodejs /app/src/assets/gtfs-static ./dist/assets/gtfs-static

# Create data and logs directories with proper permissions
RUN mkdir -p data logs temp-downloads && \
    chown -R apiuser:nodejs data logs temp-downloads

# Set the user to run the application
USER apiuser

# Expose the port the app runs on
EXPOSE ${PORT}

# Use dumb-init as an entrypoint to handle signals properly
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Define the command to run the application
CMD ["node", "dist/server.js"]
