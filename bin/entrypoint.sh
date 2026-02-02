#!/bin/sh

# log is used to log prepend timestamps to log messages
log() {
  echo "$(date) - $1"
}

# Load environment variables from the environment file
ENV_FILE="/var/run/secrets/environment"
if [ -f "$ENV_FILE" ]; then
  log "Loading environment variables from $ENV_FILE"
  while IFS= read -r line; do
    export "$line"
  done < "$ENV_FILE"
else
  log "Environment file $ENV_FILE not found"
fi

log "Running the application..."
# Run the application
exec "$@"
