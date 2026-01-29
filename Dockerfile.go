# Multi-stage build for Go binaries (api | dispatcher).
FROM golang:1.21-alpine AS build
ARG BINARY=api
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${BINARY}

FROM alpine:3.19
RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=build /out/app /app/server
EXPOSE 8080 8081
ENTRYPOINT ["/app/server"]
