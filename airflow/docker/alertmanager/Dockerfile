# Custom Alertmanager image with env var substitution in config
# Allows secrets (e.g. SLACK_WEBHOOK_URL from .env) to be injected
# into alertmanager.yml at container startup.
FROM prom/alertmanager:v0.28.1

COPY --chmod=755 entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
