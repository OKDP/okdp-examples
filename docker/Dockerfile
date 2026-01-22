#
# Copyright 2025 The OKDP Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

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