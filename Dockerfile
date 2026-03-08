# Use stable debian image as base
FROM docker.io/library/debian:trixie

ENV DEBIAN_FRONTEND=noninteractive

# Install dependent debian packages and clean up in same layer
RUN apt-get update && apt-get install -y \
    curl \
    git \
    gnupg \
    build-essential \
    sudo \
    unzip \
    jq \
    tree \
    ripgrep \
    fd-find \
    man-db \
    vim \
    tmux \
    && rm -rf /var/lib/apt/lists/*

# Create user that does coding (the developer or AI agents that run under)
ARG USER_UID=1000
ARG USER_GID=1000

RUN groupadd --gid ${USER_GID} dev \
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m dev -s /bin/bash \
    && echo "dev ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/dev \
    && chmod 0440 /etc/sudoers.d/dev

# Switch to created user to install user-level tools
USER dev
WORKDIR /home/dev

# Install Claude CLI
RUN curl -fsSL https://claude.ai/install.sh | bash

# Ensure PATH is set for interactive and non-interactive shells
RUN echo 'export PATH="/home/dev/.local/bin:${PATH}"' >> /home/dev/.bashrc

WORKDIR /workspace

CMD ["/bin/bash"]


