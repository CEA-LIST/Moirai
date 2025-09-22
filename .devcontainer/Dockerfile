FROM debian:bookworm-slim

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential pkg-config libssl-dev git sudo \
    && rm -rf /var/lib/apt/lists/*

# Install rustup + Rust system-wide
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable --no-modify-path \
    && chmod -R a+w /root/.cargo /root/.rustup

ENV PATH="/root/.cargo/bin:${PATH}"

# Create non-root user "vscode" with sudo rights
RUN useradd -m vscode && echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && mkdir -p /home/vscode/.cargo /home/vscode/.rustup \
    && cp -r /root/.cargo /home/vscode/ \
    && cp -r /root/.rustup /home/vscode/ \
    && chown -R vscode:vscode /home/vscode/.cargo /home/vscode/.rustup

USER vscode
WORKDIR /workspace

# Make sure cargo & rustc are in PATH for vscode user
ENV PATH="/home/vscode/.cargo/bin:${PATH}"
