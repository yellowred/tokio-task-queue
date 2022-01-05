FROM rust:1.57 as builder

WORKDIR /usr/src/app
ADD . .
RUN rustup component add rustfmt
RUN cargo build --release

FROM debian:buster-slim as runner

RUN apt-get update && \
    apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Etc/UTC

WORKDIR /app
COPY --from=builder /usr/src/app/target/release/taskqueue /app
EXPOSE 50051
ENTRYPOINT [ "./taskqueue" ]

