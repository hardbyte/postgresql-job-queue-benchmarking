FROM node:22-alpine

WORKDIR /app

COPY pgboss-bench/package.json .
COPY pgboss-bench/package-lock.json .
RUN npm ci --omit=dev

COPY pgboss-bench/main.js .

CMD ["node", "main.js"]
