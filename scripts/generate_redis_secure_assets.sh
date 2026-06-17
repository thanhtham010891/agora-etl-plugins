#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT_DIR=${1:-"$ROOT_DIR/.docker/redis-secure"}
REDIS_USERNAME=${REDIS_SECURE_USERNAME:-agora}
REDIS_PASSWORD=${REDIS_SECURE_PASSWORD:-agora-redis-secret}

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

cat >"$OUT_DIR/ca.cnf" <<'EOF'
[ req ]
default_bits = 4096
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[ req_distinguished_name ]
CN = Agora Redis Test CA

[ v3_ca ]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
EOF

cat >"$OUT_DIR/server.cnf" <<'EOF'
[ req ]
default_bits = 2048
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[ req_distinguished_name ]
CN = 127.0.0.1

[ v3_req ]
basicConstraints = CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = redis-secure
IP.1 = 127.0.0.1
EOF

cat >"$OUT_DIR/rogue-ca.cnf" <<'EOF'
[ req ]
default_bits = 4096
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[ req_distinguished_name ]
CN = Agora Redis Rogue Test CA

[ v3_ca ]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
EOF

openssl genrsa -out "$OUT_DIR/ca.key" 4096
openssl req -x509 -new -nodes \
  -key "$OUT_DIR/ca.key" \
  -sha256 \
  -days 3650 \
  -out "$OUT_DIR/ca.crt" \
  -config "$OUT_DIR/ca.cnf"

openssl genrsa -out "$OUT_DIR/rogue-ca.key" 4096
openssl req -x509 -new -nodes \
  -key "$OUT_DIR/rogue-ca.key" \
  -sha256 \
  -days 3650 \
  -out "$OUT_DIR/rogue-ca.crt" \
  -config "$OUT_DIR/rogue-ca.cnf"

openssl genrsa -out "$OUT_DIR/server.key" 2048
openssl req -new \
  -key "$OUT_DIR/server.key" \
  -out "$OUT_DIR/server.csr" \
  -config "$OUT_DIR/server.cnf"
openssl x509 -req \
  -in "$OUT_DIR/server.csr" \
  -CA "$OUT_DIR/ca.crt" \
  -CAkey "$OUT_DIR/ca.key" \
  -CAcreateserial \
  -out "$OUT_DIR/server.crt" \
  -days 825 \
  -sha256 \
  -extfile "$OUT_DIR/server.cnf" \
  -extensions v3_req

cat >"$OUT_DIR/redis.conf" <<'EOF'
port 0
tls-port 6379
bind 0.0.0.0
protected-mode no
tls-cert-file /etc/redis/secure/server.crt
tls-key-file /etc/redis/secure/server.key
tls-ca-cert-file /etc/redis/secure/ca.crt
tls-auth-clients no
appendonly yes
EOF

cat >"$OUT_DIR/users.acl" <<EOF
user default off
user ${REDIS_USERNAME} on >${REDIS_PASSWORD} ~* +@all
EOF

printf '%s' "$REDIS_PASSWORD" >"$OUT_DIR/password.txt"
