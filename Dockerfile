# The connector ships as its own image so it can be hotfixed on the WhatsApp protocol's
# schedule rather than on Chatwoot's: whatsmeow moves every couple of weeks, and a
# release train that had to carry a Rails app with it would be too slow to matter.
FROM golang:1.27-alpine AS build

WORKDIR /src

# Modules first, so a change to the source does not re-download the world.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# Static, because the runtime image has no libc to link against. -trimpath keeps the
# build reproducible and keeps the builder's paths out of stack traces.
RUN CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X github.com/fazer-ai/whatsapp-connector/internal/app.Version=${VERSION}" \
      -o /out/connector ./cmd/connector

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/connector /connector

# Media blobs and, from M1, the SQLite store when Postgres is not configured.
VOLUME ["/data"]
EXPOSE 8080
USER nonroot:nonroot

# The check asks the local instance, not the fleet: a fleet-wide question would restart
# every replica at once the moment Redis blinked.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/connector", "healthcheck"]

ENTRYPOINT ["/connector"]
CMD ["serve"]
