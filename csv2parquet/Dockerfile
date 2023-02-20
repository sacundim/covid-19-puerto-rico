#
# Build a Docker image for csv2parquet and json2parquet.
#
# This does a multi-architecture image, and uses Rust
# cross-compilation support to compile the foreign
# architecture natively in the host that builds the image.
#
FROM --platform=$BUILDPLATFORM rust:slim AS build
ARG TARGETARCH
COPY platform.sh /
RUN /platform.sh # writes /.platform and /.compiler
RUN apt-get update && apt-get install -y $(cat /.compiler)
RUN rustup target add $(cat /.platform)
COPY cargo-config.toml /.cargo/config.toml
WORKDIR /csv2parquet
RUN cargo install \
      --target $(cat /.platform) \
      --root /csv2parquet \
      csv2parquet \
      json2parquet

FROM debian:bullseye-slim
COPY --link --from=build \
    /csv2parquet/bin/csv2parquet \
    /csv2parquet/bin/json2parquet \
    /usr/local/bin/