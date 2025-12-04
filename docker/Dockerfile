# Minimal Trino + MinIO + Git client image
FROM alpine:3.20

# Set environment
ENV MC_VERSION=RELEASE.2024-11-21T17-21-54Z \
    TRINO_VERSION=477 \
    PATH="/usr/local/bin:${PATH}"

# Install core packages
RUN apk add --no-cache \
      bash \
      less \
      git \
      curl \
      ca-certificates \
      openjdk17-jre-headless && \
      update-ca-certificates && \
      rm -rf /var/cache/apk/*

# Install MinIO Client (mc)
RUN curl -L -o /usr/local/bin/mc \
      https://dl.min.io/client/mc/release/linux-amd64/archive/mc.${MC_VERSION} && \
    chmod +x /usr/local/bin/mc

# Install Trino CLI
RUN curl -L -o /usr/local/bin/trino \
      "https://github.com/trinodb/trino/releases/download/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}" && \
    chmod +x /usr/local/bin/trino

# Verify tools
RUN git --version
RUN mc --version
RUN trino --version

WORKDIR /data

RUN mkdir -p /data/

ENTRYPOINT ["bash"]