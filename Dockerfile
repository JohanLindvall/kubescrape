# Debian build stage: the agent links libsystemd (journald) via cgo, so it needs
# a glibc toolchain and the libsystemd headers (alpine/musl has no systemd).
FROM golang:1.26-bookworm AS build
RUN apt-get update \
	&& apt-get install -y --no-install-recommends libsystemd-dev \
	&& rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The metadata service is fully static; the agent is cgo (links libsystemd).
RUN CGO_ENABLED=0 go build -trimpath -o /kubescrape ./cmd/kubescrape \
	&& CGO_ENABLED=1 go build -trimpath -o /kubescrape-agent ./cmd/kubescrape-agent

# distroless/base has glibc (the cgo agent needs it); static-debian12 would not
# run the agent. The agent (command: /kubescrape-agent) needs root to read
# container logs; the metadata service runs as nonroot via its securityContext.
FROM gcr.io/distroless/base-debian12
# libsystemd and its runtime dependencies for the agent's journald reader.
COPY --from=build \
	/usr/lib/x86_64-linux-gnu/libsystemd.so.0* \
	/usr/lib/x86_64-linux-gnu/libcap.so.2* \
	/usr/lib/x86_64-linux-gnu/libgcrypt.so.20* \
	/usr/lib/x86_64-linux-gnu/libgpg-error.so.0* \
	/usr/lib/x86_64-linux-gnu/liblz4.so.1* \
	/usr/lib/x86_64-linux-gnu/liblzma.so.5* \
	/usr/lib/x86_64-linux-gnu/libzstd.so.1* \
	/usr/lib/x86_64-linux-gnu/
COPY --from=build /kubescrape /kubescrape
COPY --from=build /kubescrape-agent /kubescrape-agent
ENTRYPOINT ["/kubescrape"]
