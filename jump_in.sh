#!/bin/bash

CONTAINER_NAME="dev_av1_split_encode"
IMAGE_NAME="dev-env"
CLAUDE_DIR="$HOME/.claude"
SSH_DIR="$HOME/.ssh"
GNUPG_DIR="$HOME/.gnupg"

# Determine mount mode for ~/.claude
if [ "$CLAUDE_CONFIG_WRITABLE" = "1" ]; then
    CLAUDE_MOUNT_OPTS=":Z"
else
    CLAUDE_MOUNT_OPTS=":ro"
    echo "⚠️  ~/.claude is mounted read-only inside the container."
    echo "   Set CLAUDE_CONFIG_WRITABLE=1 to allow writes."
fi

# Build if image doesn't exist
if ! podman image exists "$IMAGE_NAME"; then
    podman build -t "$IMAGE_NAME" .
fi

if [ ! -d "$CLAUDE_DIR" ]; then
    echo "Error: $CLAUDE_DIR does not exist. Run 'claude' on the host first to set it up."
    exit 1
fi

if [ ! -d "$SSH_DIR" ]; then
    echo "Warning: $SSH_DIR does not exist. SSH keys will not be available in the container."
fi

if [ ! -d "$GNUPG_DIR" ]; then
    echo "Warning: $GNUPG_DIR does not exist. GPG keys will not be available in the container."
fi

# Build SSH and GPG mount args conditionally
SSH_MOUNT=()
GNUPG_MOUNT=()
if [ -d "$SSH_DIR" ]; then
    SSH_MOUNT=(-v "$SSH_DIR:/home/dev/.ssh:ro,Z")
fi
if [ -d "$GNUPG_DIR" ]; then
    GNUPG_MOUNT=(-v "$GNUPG_DIR:/home/dev/.gnupg:ro,Z")
fi

exec podman run -it --rm \
    --name "$CONTAINER_NAME" \
    --userns=keep-id \
    --network=host \
    --user 1000:1000 \
    -v .:/workspace:Z \
    -v "$CLAUDE_DIR:/home/dev/.claude${CLAUDE_MOUNT_OPTS}" \
    -v "$HOME/.claude.json:/home/dev/.claude.json:Z" \
    "${SSH_MOUNT[@]}" \
    "${GNUPG_MOUNT[@]}" \
    -e TERM=xterm-256color \
    "$IMAGE_NAME" \
    bash

