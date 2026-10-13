FROM golang:1.25.0-alpine AS build

RUN apk add --no-cache ca-certificates git
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/service

FROM alpine:3.22

RUN apk add --no-cache ca-certificates curl && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/service /app/service
COPY migrations /app/migrations

USER app
EXPOSE 8080 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD components="${COMPONENTS:-http}"; case ",$components," in *,http,*) http_addr="${HTTP_ADDR:-:${APP_HTTP_PORT:-8080}}"; port="${http_addr##*:}"; curl -fsS "http://127.0.0.1:${port}/health/live" ;; *) kill -0 1 ;; esac
ENTRYPOINT ["/app/service"]
