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

# Build SSH and GPG mount args conditionally
SSH_MOUNT=()
SSH_AGENT_MOUNT=()
GNUPG_MOUNT=()
GPG_AGENT_MOUNT=()
if [ -d "$SSH_DIR" ]; then
    SSH_MOUNT=(-v "$SSH_DIR:/home/dev/.ssh:ro,Z")
else
    echo "Warning: $SSH_DIR does not exist. SSH keys will not be available in the container."
fi

# Forward the host ssh-agent socket so the container can authenticate without re-entering passphrases
if [ -S "$SSH_AUTH_SOCK" ]; then
    SSH_AGENT_MOUNT=(-v "$SSH_AUTH_SOCK:/run/ssh-agent.sock" -e SSH_AUTH_SOCK=/run/ssh-agent.sock)
else
    echo "Warning: ssh-agent socket not found (SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-<unset>}). SSH authentication may require passphrase entry."
fi
if [ -d "$GNUPG_DIR" ]; then
    GNUPG_MOUNT=(-v "$GNUPG_DIR:/home/dev/.gnupg:ro,Z")
else
    echo "Warning: $GNUPG_DIR does not exist. GPG keys will not be available in the container."
fi

# Forward the host gpg-agent socket so the container can sign without re-entering a passphrase
GPG_AGENT_SOCK=$(gpgconf --list-dirs agent-socket 2>/dev/null)
if [ -S "$GPG_AGENT_SOCK" ]; then
    GPG_AGENT_MOUNT=(-v "$GPG_AGENT_SOCK:/home/dev/.gnupg/S.gpg-agent")
else
    echo "Warning: gpg-agent socket not found at ${GPG_AGENT_SOCK:-<unknown>}. GPG signing may require passphrase entry."
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
    "${SSH_AGENT_MOUNT[@]}" \
    "${GNUPG_MOUNT[@]}" \
    "${GPG_AGENT_MOUNT[@]}" \
    -e TERM=xterm-256color \
    "$IMAGE_NAME" \
    bash
