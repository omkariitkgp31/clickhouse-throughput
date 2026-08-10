FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json tsconfig.json ./
COPY src/shared ./src/shared
COPY src/services/ingestion-api ./src/services/ingestion-api

RUN npm install
RUN npm run build --workspace=src/shared
RUN npm run build --workspace=src/services/ingestion-api

FROM node:20-alpine
WORKDIR /app

COPY package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/src/shared ./src/shared
COPY --from=builder /app/src/services/ingestion-api ./src/services/ingestion-api

WORKDIR /app/src/services/ingestion-api
EXPOSE 8080
CMD ["node", "dist/index.js"]
