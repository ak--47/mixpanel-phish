#!/bin/bash
set -e

# Enhanced base64 function that produces URL-safe output
base64url_encode() {
    if base64 --version 2>&1 | grep -q 'GNU coreutils'; then
        base64 -w0 | tr '/+' '_-' | tr -d '='
    else
        base64 | tr -d '\n' | tr '/+' '_-' | tr -d '='
    fi
}

if [ ! -f "service-account.json" ]; then
    echo "Error: service-account.json not found"
    exit 1
fi

# Read required values from service account file
PRIVATE_KEY_ID=$(jq -r .private_key_id service-account.json)
PRIVATE_KEY=$(jq -r .private_key service-account.json)
CLIENT_EMAIL=$(jq -r .client_email service-account.json)

# Create temporary file for private key
PRIVATE_KEY_FILE=$(mktemp)
chmod 600 "$PRIVATE_KEY_FILE"
echo "$PRIVATE_KEY" > "$PRIVATE_KEY_FILE"

NOW=$(($(date +%s)))
EXPIRATION=$((NOW + (3600)))

# Create JWT header
JWT_HEADER=$(echo -n '{"alg":"RS256","typ":"JWT","kid":"'$PRIVATE_KEY_ID'"}' | base64url_encode)

# Create JWT payload for ID token request
# Note: For Cloud Run, we only specify target_audience, not scope
JWT_PAYLOAD=$(echo -n '{
    "iss": "'$CLIENT_EMAIL'",
    "sub": "'$CLIENT_EMAIL'",
    "aud": "https://oauth2.googleapis.com/token",
    "target_audience": "https://mixpanel-phish-lmozz6xkha-uc.a.run.app",
    "exp": '$EXPIRATION',
    "iat": '$NOW'
}' | base64url_encode)

# Create JWT signature
JWT_SIGNATURE=$(echo -n "${JWT_HEADER}.${JWT_PAYLOAD}" | \
    openssl dgst -binary -sha256 -sign "$PRIVATE_KEY_FILE" | \
    base64url_encode)

# Combine to create final JWT
JWT="${JWT_HEADER}.${JWT_PAYLOAD}.${JWT_SIGNATURE}"

# Exchange JWT for ID token, note the different parameters here
RESPONSE=$(curl -s -X POST https://oauth2.googleapis.com/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer" \
    -d "assertion=$JWT")

# Check for errors in response
if echo "$RESPONSE" | jq -e 'has("error")' > /dev/null; then
    echo "Error from Google OAuth server:"
    echo "$RESPONSE" | jq -r '.error_description // .error'
    rm "$PRIVATE_KEY_FILE"
    exit 1
fi

# Save token to credentials file
echo "$RESPONSE" | jq '{
    "id_token": .id_token,
    "generated_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
}' > credentials.json

rm "$PRIVATE_KEY_FILE"

if [ -f "credentials.json" ] && [ "$(jq -r .id_token credentials.json)" != "null" ]; then
    echo "Successfully generated ID token and saved to credentials.json"
    echo "Token will expire in $(jq -r .expires_in credentials.json) seconds"
	echo "$RESPONSE" | jq '.'
else
    echo "Error: Token generation failed"
    echo "Response from server:"
    echo "$RESPONSE" | jq '.'
    exit 1
fi