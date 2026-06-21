FROM node:22-alpine AS frontend
WORKDIR /web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party/ ./third_party/
RUN go mod download
COPY . .
COPY --from=frontend /web/build ./internal/frontend/build
RUN go build -o /out/drakkar ./cmd/drakkar

FROM alpine:3.22
RUN apk add --no-cache ca-certificates fuse3 par2cmdline 7zip tzdata
WORKDIR /app
COPY --from=build /out/drakkar /app/drakkar
COPY --from=build /src/migrations /app/migrations
EXPOSE 8080
ENTRYPOINT ["/app/drakkar"]
