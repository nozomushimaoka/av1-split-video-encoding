#!/bin/bash

CONTAINER_NAME="dev_av1_split_encode"
IMAGE_NAME="dev-env"
CLAUDE_DIR="$HOME/.claude"
SSH_DIR="$HOME/.ssh"
GNUPG_DIR="$HOME/.gnupg"

OPT_SSH=0
OPT_GPG=0
OPT_CLAUDE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ssh)    OPT_SSH=1 ;;
        --gpg)    OPT_GPG=1 ;;
        --claude) OPT_CLAUDE=1 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
    shift
done

# Build if image doesn't exist
if ! podman image exists "$IMAGE_NAME"; then
    podman build -t "$IMAGE_NAME" .
fi

SSH_MOUNT=()
SSH_AGENT_MOUNT=()
if [[ $OPT_SSH -eq 1 ]]; then
    echo "⚠️  --ssh: SSH keys and agent will be forwarded. Claude Code can authenticate to any SSH host as you."
    if [ -d "$SSH_DIR" ]; then
        SSH_MOUNT=(-v "$SSH_DIR:/home/dev/.ssh:ro,Z")
    else
        echo "Warning: --ssh passed but $SSH_DIR does not exist."
    fi
    if [ -S "$SSH_AUTH_SOCK" ]; then
        SSH_AGENT_MOUNT=(-v "$SSH_AUTH_SOCK:/run/ssh-agent.sock" -e SSH_AUTH_SOCK=/run/ssh-agent.sock)
    else
        echo "Warning: --ssh passed but ssh-agent socket not found (SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-<unset>})."
    fi
fi

GNUPG_MOUNT=()
GPG_AGENT_MOUNT=()
if [[ $OPT_GPG -eq 1 ]]; then
    echo "⚠️  --gpg: GPG keys and agent will be forwarded. Claude Code can sign data as you."
    if [ -d "$GNUPG_DIR" ]; then
        GNUPG_MOUNT=(-v "$GNUPG_DIR:/home/dev/.gnupg:ro,Z")
    else
        echo "Warning: --gpg passed but $GNUPG_DIR does not exist."
    fi
    GPG_AGENT_SOCK=$(gpgconf --list-dirs agent-socket 2>/dev/null)
    if [ -S "$GPG_AGENT_SOCK" ]; then
        GPG_AGENT_MOUNT=(-v "$GPG_AGENT_SOCK:/home/dev/.gnupg/S.gpg-agent")
    else
        echo "Warning: --gpg passed but gpg-agent socket not found at ${GPG_AGENT_SOCK:-<unknown>}."
    fi
fi

CLAUDE_MOUNT=()
CLAUDE_JSON_MOUNT=()
if [[ $OPT_CLAUDE -eq 1 ]]; then
    echo "⚠️  --claude: ~/.claude and ~/.claude.json are mounted read-write. Claude Code can modify its config, hooks, and API credentials."
    if [ -d "$CLAUDE_DIR" ]; then
        CLAUDE_MOUNT=(-v "$CLAUDE_DIR:/home/dev/.claude:Z")
    else
        echo "Warning: --claude passed but $CLAUDE_DIR does not exist. Run 'claude' on the host first."
    fi
    if [ -f "$HOME/.claude.json" ]; then
        CLAUDE_JSON_MOUNT=(-v "$HOME/.claude.json:/home/dev/.claude.json:Z")
    fi
fi

podman build -t "$IMAGE_NAME" . || { echo "Build failed"; exit 1; }

exec podman run -it --rm \
    --name "$CONTAINER_NAME" \
    --userns=keep-id \
    --network=host \
    --user 1000:1000 \
    -v .:/workspace:Z \
    "${CLAUDE_MOUNT[@]}" \
    "${CLAUDE_JSON_MOUNT[@]}" \
    "${SSH_MOUNT[@]}" \
    "${SSH_AGENT_MOUNT[@]}" \
    "${GNUPG_MOUNT[@]}" \
    "${GPG_AGENT_MOUNT[@]}" \
    -e TERM=xterm-256color \
    "$IMAGE_NAME" \
    bash
