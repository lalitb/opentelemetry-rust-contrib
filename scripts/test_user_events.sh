set -eu

echo "Running user_events tests"
sudo -E ~/.cargo/bin/cargo test --manifest-path=opentelemetry-user-events-logs/Cargo.toml -- --nocapture --ignored