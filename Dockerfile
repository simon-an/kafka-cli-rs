FROM lukemathwalker/cargo-chef:latest-rust-slim-bookworm AS chef
WORKDIR /app

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y ca-certificates

FROM chef AS planner
RUN cargo install cargo-chef 
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

## Needed when there are private git dependencies in cargo toml
# ARG GIT_CREDENTIALS
# ARG GIT_URL
# ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# RUN git config --global credential.helper store
# RUN echo "https://${GIT_CREDENTIALS}@GIT_URL" > ~/.git-credentials

RUN cargo chef cook --release --recipe-path recipe.json

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y cmake

COPY . .

# COPY --from=cacher /app/target target
# COPY --from=cacher $CARGO_HOME $CARGO_HOME
RUN cargo build --release --bin kafka-cli

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y ca-certificates

WORKDIR /app
COPY --from=builder /app/target/release/kafka-cli /usr/local/bin
USER 1000
CMD ["-V"]
ENTRYPOINT ["/usr/local/bin/kafka-cli"]
