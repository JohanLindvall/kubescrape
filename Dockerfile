FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /kubescrape ./cmd/kubescrape \
	&& CGO_ENABLED=0 go build -trimpath -o /kubescrape-agent ./cmd/kubescrape-agent

# The agent (command: /kubescrape-agent) needs root to read container logs;
# the metadata service runs as nonroot via its manifest's securityContext.
FROM gcr.io/distroless/static-debian12
COPY --from=build /kubescrape /kubescrape
COPY --from=build /kubescrape-agent /kubescrape-agent
ENTRYPOINT ["/kubescrape"]
