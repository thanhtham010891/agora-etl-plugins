#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT_DIR=${1:-"$ROOT_DIR/.docker/kafka-secure"}
STORE_PASSWORD=${KAFKA_CERT_STORE_PASSWORD:-changeit}
SCRAM_USERNAME=${KAFKA_SCRAM_USERNAME:-agora}
SCRAM_PASSWORD=${KAFKA_SCRAM_PASSWORD:-agora-scram-secret}
SCHEMA_REGISTRY_USERNAME=${SCHEMA_REGISTRY_BASIC_AUTH_USERNAME:-agora}
SCHEMA_REGISTRY_PASSWORD=${SCHEMA_REGISTRY_BASIC_AUTH_PASSWORD:-agora-registry-secret}
KEYTOOL_BIN=${KEYTOOL_BIN:-}

if [ -z "$KEYTOOL_BIN" ] && [ -x /usr/local/opt/openjdk@17/bin/keytool ]; then
  KEYTOOL_BIN=/usr/local/opt/openjdk@17/bin/keytool
fi
if [ -z "$KEYTOOL_BIN" ]; then
  KEYTOOL_BIN=$(command -v keytool 2>/dev/null || true)
fi
if [ -z "$KEYTOOL_BIN" ]; then
  echo "keytool was not found. Set KEYTOOL_BIN=/path/to/keytool and retry." >&2
  exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

cat >"$OUT_DIR/ca.cnf" <<'EOF'
[ req ]
default_bits = 4096
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[ req_distinguished_name ]
CN = Agora Kafka Test CA

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
DNS.2 = kafka-secure
IP.1 = 127.0.0.1
EOF

cat >"$OUT_DIR/client.cnf" <<'EOF'
[ req ]
default_bits = 2048
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[ req_distinguished_name ]
CN = agora-client

[ v3_req ]
basicConstraints = CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = agora-client
EOF

cat >"$OUT_DIR/rogue-ca.cnf" <<'EOF'
[ req ]
default_bits = 4096
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[ req_distinguished_name ]
CN = Agora Rogue Test CA

[ v3_ca ]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
EOF

cat >"$OUT_DIR/rogue-client.cnf" <<'EOF'
[ req ]
default_bits = 2048
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[ req_distinguished_name ]
CN = agora-rogue-client

[ v3_req ]
basicConstraints = CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = agora-rogue-client
EOF

cat >"$OUT_DIR/schema-registry.cnf" <<'EOF'
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
DNS.2 = schema-registry-secure
IP.1 = 127.0.0.1
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

openssl genrsa -out "$OUT_DIR/client.key" 2048
openssl req -new \
  -key "$OUT_DIR/client.key" \
  -out "$OUT_DIR/client.csr" \
  -config "$OUT_DIR/client.cnf"
openssl x509 -req \
  -in "$OUT_DIR/client.csr" \
  -CA "$OUT_DIR/ca.crt" \
  -CAkey "$OUT_DIR/ca.key" \
  -CAcreateserial \
  -out "$OUT_DIR/client.crt" \
  -days 825 \
  -sha256 \
  -extfile "$OUT_DIR/client.cnf" \
  -extensions v3_req

openssl genrsa -out "$OUT_DIR/rogue-client.key" 2048
openssl req -new \
  -key "$OUT_DIR/rogue-client.key" \
  -out "$OUT_DIR/rogue-client.csr" \
  -config "$OUT_DIR/rogue-client.cnf"
openssl x509 -req \
  -in "$OUT_DIR/rogue-client.csr" \
  -CA "$OUT_DIR/rogue-ca.crt" \
  -CAkey "$OUT_DIR/rogue-ca.key" \
  -CAcreateserial \
  -out "$OUT_DIR/rogue-client.crt" \
  -days 825 \
  -sha256 \
  -extfile "$OUT_DIR/rogue-client.cnf" \
  -extensions v3_req

openssl genrsa -out "$OUT_DIR/schema-registry.key" 2048
openssl req -new \
  -key "$OUT_DIR/schema-registry.key" \
  -out "$OUT_DIR/schema-registry.csr" \
  -config "$OUT_DIR/schema-registry.cnf"
openssl x509 -req \
  -in "$OUT_DIR/schema-registry.csr" \
  -CA "$OUT_DIR/ca.crt" \
  -CAkey "$OUT_DIR/ca.key" \
  -CAcreateserial \
  -out "$OUT_DIR/schema-registry.crt" \
  -days 825 \
  -sha256 \
  -extfile "$OUT_DIR/schema-registry.cnf" \
  -extensions v3_req

openssl pkcs12 -export \
  -in "$OUT_DIR/server.crt" \
  -inkey "$OUT_DIR/server.key" \
  -certfile "$OUT_DIR/ca.crt" \
  -name kafka-broker \
  -out "$OUT_DIR/server.keystore.p12" \
  -passout "pass:$STORE_PASSWORD"

openssl pkcs12 -export \
  -in "$OUT_DIR/client.crt" \
  -inkey "$OUT_DIR/client.key" \
  -certfile "$OUT_DIR/ca.crt" \
  -name agora-client \
  -out "$OUT_DIR/client.keystore.p12" \
  -passout "pass:$STORE_PASSWORD"

"$KEYTOOL_BIN" -importcert -noprompt \
  -alias CARoot \
  -file "$OUT_DIR/ca.crt" \
  -keystore "$OUT_DIR/server.truststore.p12" \
  -storetype PKCS12 \
  -storepass "$STORE_PASSWORD"

"$KEYTOOL_BIN" -importcert -noprompt \
  -alias CARoot \
  -file "$OUT_DIR/ca.crt" \
  -keystore "$OUT_DIR/client.truststore.p12" \
  -storetype PKCS12 \
  -storepass "$STORE_PASSWORD"

cat >"$OUT_DIR/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka-secure:29093
listeners=INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,SASL_SSL://0.0.0.0:9093,SSL://0.0.0.0:9094,SSL_INTERNAL://0.0.0.0:29094
advertised.listeners=INTERNAL://kafka-secure:29092,SASL_SSL://127.0.0.1:19093,SSL://127.0.0.1:19094,SSL_INTERNAL://kafka-secure:29094
listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL,SSL:SSL,SSL_INTERNAL:SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
log.dirs=/var/lib/kafka/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
num.partitions=1
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/server.keystore.p12
ssl.keystore.password=$STORE_PASSWORD
ssl.key.password=$STORE_PASSWORD
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/server.truststore.p12
ssl.truststore.password=$STORE_PASSWORD
listener.name.ssl.ssl.client.auth=required
listener.name.ssl_internal.ssl.client.auth=required
listener.name.sasl_ssl.ssl.client.auth=none
sasl.enabled.mechanisms=SCRAM-SHA-256
listener.name.sasl_ssl.sasl.enabled.mechanisms=SCRAM-SHA-256
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
EOF

cat >"$OUT_DIR/client-mtls.properties" <<EOF
security.protocol=SSL
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/client.truststore.p12
ssl.truststore.password=$STORE_PASSWORD
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/client.keystore.p12
ssl.keystore.password=$STORE_PASSWORD
ssl.key.password=$STORE_PASSWORD
ssl.endpoint.identification.algorithm=
EOF

cat >"$OUT_DIR/scram-password.txt" <<EOF
$SCRAM_PASSWORD
EOF

cat >"$OUT_DIR/security.env" <<EOF
AGORA_TEST_KAFKA_SCRAM_USERNAME=$SCRAM_USERNAME
AGORA_TEST_KAFKA_SCRAM_PASSWORD_FILE=$OUT_DIR/scram-password.txt
AGORA_TEST_KAFKA_CA_FILE=$OUT_DIR/ca.crt
AGORA_TEST_KAFKA_CLIENT_CERT_FILE=$OUT_DIR/client.crt
AGORA_TEST_KAFKA_CLIENT_KEY_FILE=$OUT_DIR/client.key
AGORA_TEST_SCHEMA_REGISTRY_URL=https://127.0.0.1:18081
AGORA_TEST_SCHEMA_REGISTRY_MTLS_URL=https://127.0.0.1:18443
AGORA_TEST_SCHEMA_REGISTRY_USERNAME=$SCHEMA_REGISTRY_USERNAME
AGORA_TEST_SCHEMA_REGISTRY_PASSWORD_FILE=$OUT_DIR/schema-registry-password.txt
EOF

cat >"$OUT_DIR/schema-registry-password.txt" <<EOF
$SCHEMA_REGISTRY_PASSWORD
EOF

htpasswd -bcB "$OUT_DIR/schema-registry.htpasswd" \
  "$SCHEMA_REGISTRY_USERNAME" \
  "$SCHEMA_REGISTRY_PASSWORD" >/dev/null 2>&1

cat >"$OUT_DIR/schema-registry-nginx.conf" <<'EOF'
events {}

http {
  server {
    listen 8443 ssl;
    server_name schema-registry-secure localhost;

    ssl_certificate /etc/nginx/secrets/schema-registry.crt;
    ssl_certificate_key /etc/nginx/secrets/schema-registry.key;

    auth_basic "Agora Schema Registry";
    auth_basic_user_file /etc/nginx/secrets/schema-registry.htpasswd;

    location / {
      proxy_pass http://schema-registry:8081;
      proxy_set_header Host $host;
      proxy_set_header X-Forwarded-Proto https;
      proxy_set_header X-Forwarded-For $remote_addr;
    }
  }

  server {
    listen 8444 ssl;
    server_name schema-registry-secure-mtls localhost;

    ssl_certificate /etc/nginx/secrets/schema-registry.crt;
    ssl_certificate_key /etc/nginx/secrets/schema-registry.key;
    ssl_client_certificate /etc/nginx/secrets/ca.crt;
    ssl_verify_client on;

    auth_basic "Agora Schema Registry";
    auth_basic_user_file /etc/nginx/secrets/schema-registry.htpasswd;

    location / {
      proxy_pass http://schema-registry:8081;
      proxy_set_header Host $host;
      proxy_set_header X-Forwarded-Proto https;
      proxy_set_header X-Forwarded-For $remote_addr;
    }
  }
}
EOF

chmod 600 \
  "$OUT_DIR/ca.key" \
  "$OUT_DIR/rogue-ca.key" \
  "$OUT_DIR/server.key" \
  "$OUT_DIR/client.key" \
  "$OUT_DIR/rogue-client.key" \
  "$OUT_DIR/schema-registry.key"

printf 'Generated Kafka secure assets in %s\n' "$OUT_DIR"
