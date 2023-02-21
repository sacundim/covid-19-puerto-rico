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
# We very specifically want v0.6.0 of these tools, I've
# confirmed that v0.6.1 won't parse our Covid19Datos-V2
# CSVs, e.g.:
#
# Error: ArrowError("underlying Arrow error: Parser error:
# Error while parsing value 2020-03-09 00:00:00 for column
# 6 at line 2")
RUN cargo install \
      --target $(cat /.platform) \
      --root /csv2parquet \
      csv2parquet@0.6.0 \
      json2parquet@0.6.0

FROM debian:bullseye-slim
COPY --link --from=build \
    /csv2parquet/bin/csv2parquet \
    /csv2parquet/bin/json2parquet \
    /usr/local/bin/