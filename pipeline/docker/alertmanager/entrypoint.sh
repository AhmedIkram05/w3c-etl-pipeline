#!/bin/sh
set -e

# Substitute SLACK_WEBHOOK_URL into the Alertmanager config at runtime.
# The original file (mounted read-only) stays untouched; the processed
# copy lives in /tmp so it can be written without mount restrictions.
# Uses | as sed delimiter to avoid conflicts with URL slashes.
sed "s|\$SLACK_WEBHOOK_URL|${SLACK_WEBHOOK_URL}|g" \
  /etc/alertmanager/alertmanager.yml > /tmp/alertmanager.yml

# Launch Alertmanager with the processed config, forwarding any
# additional arguments from the docker-compose command.
exec /bin/alertmanager --config.file=/tmp/alertmanager.yml "$@"
