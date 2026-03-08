#!/bin/bash

CONTAINER_NAME="dev_av1_split_encode"
IMAGE_NAME="dev-env"

# Build if image doesn't exist
if ! podman image exists "$IMAGE_NAME"; then
    podman build -t "$IMAGE_NAME" .
fi

# Start if not already running
if ! podman container exists "$CONTAINER_NAME"; then
    podman run -d \
        --name "$CONTAINER_NAME" \
        --userns=keep-id \
        --network=host \
        --user 1000:1000 \
        -v .:/workspace:Z \
        -v claude-config:/home/dev/.claude \
        -e TERM=xterm-256color \
        "$IMAGE_NAME" \
        tail -f /dev/null
fi

# Jump in
exec podman exec -it "$CONTAINER_NAME" bash

