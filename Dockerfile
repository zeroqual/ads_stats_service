# syntax=docker/dockerfile:1

FROM golang:1.27 AS deps
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

FROM deps AS build
ARG SERVICE
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    if [ -z "${SERVICE}" ]; then echo "build arg SERVICE is required" >&2; exit 1; fi; \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM golang:1.27 AS goose
ARG GOOSE_VERSION=latest
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go install \
      -tags='no_postgres no_mysql no_sqlite3 no_mssql no_vertica no_ydb no_libsql' \
      github.com/pressly/goose/v3/cmd/goose@${GOOSE_VERSION}

FROM gcr.io/distroless/static-debian12:nonroot AS migrate
COPY --from=goose /go/bin/goose /goose
COPY migrations /migrations
ENV GOOSE_DRIVER=clickhouse \
    GOOSE_MIGRATION_DIR=/migrations
ENTRYPOINT ["/goose"]
CMD ["up"]

FROM gcr.io/distroless/static-debian12:nonroot AS app
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
