FROM rust:1.30
RUN USER=root cargo new --bin mid-player-broker
WORKDIR /mid-player-broker
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs
COPY ./src ./src
RUN rm ./target/release/deps/mid_player_broker*
RUN cargo build --release

CMD ["./target/release/mid-player-broker"]