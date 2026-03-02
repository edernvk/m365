FROM node:20-alpine

LABEL maintainer="M365 Migrator"
LABEL description="Microsoft 365 Tenant-to-Tenant Migration Tool"

# Create app directory
WORKDIR /app

# Install dependencies
COPY package.json ./
RUN npm install --omit=dev

# Copy source
COPY src/ ./src/

# Create directories for volumes
RUN mkdir -p /app/logs /app/data

# Default command
CMD ["node", "src/migrator.js"]
