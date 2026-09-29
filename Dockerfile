# Use official lightweight Node.js LTS image
FROM node:20-alpine

# Set working directory inside container
WORKDIR /usr/src/app

# Copy package descriptors
COPY package*.json ./

# Install production dependencies
RUN npm ci --only=production

# Copy source code and config
COPY . .

# Expose Express API port
EXPOSE 3000

# Set environment
ENV NODE_ENV=production
ENV PORT=3000

# Default command starts the Express API server
CMD ["node", "src/server.js"]
