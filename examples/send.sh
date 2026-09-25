#!/usr/bin/env bash
# Send the example webhooks to a local Relay (default http://127.0.0.1:8000).
# Uses the dev secrets from config/relay.yaml; requires `relay` on PATH for the HMAC signature.
set -euo pipefail
BASE="${RELAY_URL:-http://127.0.0.1:8000}"
DIR="$(cd "$(dirname "$0")" && pwd)"
FORM="Content-Type: application/x-www-form-urlencoded"

echo "--> Tilda form";   curl -sS -X POST "$BASE/webhooks/tilda-site" -H "$FORM" --data @"$DIR/tilda-form.txt"; echo
echo "--> Tilda order";  curl -sS -X POST "$BASE/webhooks/tilda-site" -H "Content-Type: application/json" --data-binary @"$DIR/tilda-order.json"; echo
echo "--> amoCRM (won)"; curl -sS -X POST "$BASE/webhooks/amocrm?token=dev-amocrm-token" -H "$FORM" --data @"$DIR/amocrm-status.txt"; echo
echo "--> Bitrix24";     curl -sS -X POST "$BASE/webhooks/bitrix" -H "$FORM" --data @"$DIR/bitrix-lead.txt"; echo
SIG="$(relay sign --secret dev-partner-secret --file "$DIR/partner-event.json")"
echo "--> Partner (HMAC)"; curl -sS -X POST "$BASE/webhooks/partner-api" -H "Content-Type: application/json" -H "X-Relay-Signature: $SIG" --data-binary @"$DIR/partner-event.json"; echo
echo "--> Duplicate Tilda form (expect 200 + duplicates)"; curl -sS -X POST "$BASE/webhooks/tilda-site" -H "$FORM" --data @"$DIR/tilda-form.txt"; echo
