FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json tsconfig.json ./
COPY src/shared ./src/shared
COPY src/services/stream-processor ./src/services/stream-processor

RUN npm install
RUN npm run build --workspace=src/shared
RUN npm run build --workspace=src/services/stream-processor

FROM node:20-alpine
WORKDIR /app

COPY package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/src/shared ./src/shared
COPY --from=builder /app/src/services/stream-processor ./src/services/stream-processor

WORKDIR /app/src/services/stream-processor
CMD ["node", "dist/index.js"]
