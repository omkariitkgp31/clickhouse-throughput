FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json tsconfig.json ./
COPY src/shared ./src/shared
COPY src/services/query-api ./src/services/query-api

RUN npm install
RUN npm run build --workspace=src/shared
RUN npm run build --workspace=src/services/query-api

FROM node:20-alpine
WORKDIR /app

COPY package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/src/shared ./src/shared
COPY --from=builder /app/src/services/query-api ./src/services/query-api

WORKDIR /app/src/services/query-api
EXPOSE 8081
CMD ["node", "dist/index.js"]
