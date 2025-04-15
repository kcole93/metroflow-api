# Dockerfile

# ---- Builder Stage ----
FROM node:18-alpine AS builder

WORKDIR /app

# Install pnpm globally within the builder
RUN npm install -g pnpm

# Copy package files
COPY package.json pnpm-lock.yaml ./

# Install ALL dependencies (including dev for build)
# Using --frozen-lockfile ensures exact versions from lockfile
RUN pnpm install --frozen-lockfile

# Copy the rest of the application code
COPY . .

# Build TypeScript to JavaScript in /app/dist
RUN pnpm run build

# Prune dev dependencies
RUN pnpm prune --prod


# ---- Final Stage ----
FROM node:18-alpine AS final

WORKDIR /app

# Copy production node_modules from builder
COPY --from=builder /app/node_modules ./node_modules

# Copy built application from builder
COPY --from=builder /app/dist ./dist

# Copy assets needed at runtime (proto files, geojson)
# Adjust paths if they are outside 'src' after build
COPY --from=builder /app/src/assets ./dist/assets
# ** IMPORTANT: DO NOT COPY src/assets/gtfs-static here **
# This will be handled by a Docker volume

# Expose the port the app runs on
EXPOSE 3000

# Define the command to run the application
# Use node directly to run the compiled JS
CMD ["node", "dist/server.js"]
