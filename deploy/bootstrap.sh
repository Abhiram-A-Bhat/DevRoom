#!/usr/bin/env bash
# Run from the repo root on a fresh Ubuntu server:
#   sudo bash deploy/bootstrap.sh https://your-frontend.vercel.app
set -euo pipefail

FRONTEND_URL="${1:?Usage: sudo bash deploy/bootstrap.sh https://your-frontend.vercel.app}"
FRONTEND_URL="${FRONTEND_URL%/}"

if ! command -v docker >/dev/null 2>&1; then
  apt-get update
  apt-get install -y docker.io docker-compose-v2 openssl curl
  systemctl enable --now docker
fi

if [ ! -f .env ]; then
  PUBLIC_IP="$(curl -4fsS https://api.ipify.org)"
  umask 077
  cat > .env <<EOF
DB_PASSWORD=$(openssl rand -hex 24)
API_DOMAIN=${PUBLIC_IP//./-}.sslip.io
CORS_ORIGIN=${FRONTEND_URL}
RATE_LIMIT_MAX=20
EOF
else
  sed -i "s|^CORS_ORIGIN=.*|CORS_ORIGIN=${FRONTEND_URL}|" .env
fi

docker compose -f docker-compose.prod.yml up -d --build

API_DOMAIN="$(grep '^API_DOMAIN=' .env | cut -d= -f2)"
echo "Waiting for https://${API_DOMAIN}/health"
for _ in $(seq 1 60); do
  if curl -fsS "https://${API_DOMAIN}/health" >/dev/null 2>&1; then
    echo "Backend is live: https://${API_DOMAIN}"
    exit 0
  fi
  sleep 5
done

echo "Backend did not become healthy. Check: docker compose -f docker-compose.prod.yml logs --tail=80" >&2
exit 1
