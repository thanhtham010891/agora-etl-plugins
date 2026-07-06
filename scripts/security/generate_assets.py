#!/usr/bin/env python3
"""Generate local security assets for integration topologies."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
from textwrap import dedent

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_KAFKA_OUT_DIR = REPO_ROOT / ".docker" / "kafka-secure"
DEFAULT_REDIS_OUT_DIR = REPO_ROOT / ".docker" / "redis-secure"
BREW_KEYTOOL = Path("/usr/local/opt/openjdk@17/bin/keytool")

KAFKA_CA_CONFIG = """
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
"""

KAFKA_SERVER_CONFIG = """
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
"""

KAFKA_CLIENT_CONFIG = """
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
"""

KAFKA_ROGUE_CA_CONFIG = """
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
"""

KAFKA_ROGUE_CLIENT_CONFIG = """
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
"""

KAFKA_SCHEMA_REGISTRY_CONFIG = """
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
"""

KAFKA_SCHEMA_REGISTRY_NGINX_CONFIG = """
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
"""

REDIS_CA_CONFIG = """
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
"""

REDIS_SERVER_CONFIG = """
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
"""

REDIS_ROGUE_CA_CONFIG = """
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
"""

REDIS_SERVER_RUNTIME_CONFIG = """
port 0
tls-port 6379
bind 0.0.0.0
protected-mode no
tls-cert-file /etc/redis/secure/server.crt
tls-key-file /etc/redis/secure/server.key
tls-ca-cert-file /etc/redis/secure/ca.crt
tls-auth-clients no
appendonly yes
"""


def _clean_text(value: str) -> str:
    return dedent(value).lstrip("\n").rstrip() + "\n"


def _write_text(path: Path, content: str) -> None:
    path.write_text(_clean_text(content), encoding="utf-8")


def _write_secret(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)


def _run(command: list[str]) -> None:
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def _reset_output_dir(output_dir: Path) -> None:
    resolved = output_dir.resolve()
    if resolved in {resolved.anchor and Path(resolved.anchor), REPO_ROOT.resolve()}:
        raise SystemExit(f"Refusing to reset unsafe output directory: {resolved}")
    shutil.rmtree(resolved, ignore_errors=True)
    resolved.mkdir(parents=True, exist_ok=True)


def _resolve_keytool() -> str:
    env_keytool = os.getenv("KEYTOOL_BIN", "").strip()
    if env_keytool:
        return env_keytool
    if BREW_KEYTOOL.exists():
        return str(BREW_KEYTOOL)
    keytool = shutil.which("keytool")
    if keytool:
        return keytool
    raise SystemExit("keytool was not found. Set KEYTOOL_BIN=/path/to/keytool and retry.")


def _generate_signed_cert(
    *,
    output_dir: Path,
    name: str,
    config_name: str,
    config_text: str,
    ca_cert: str,
    ca_key: str,
) -> None:
    _write_text(output_dir / config_name, config_text)
    _run(["openssl", "genrsa", "-out", str(output_dir / f"{name}.key"), "2048"])
    _run(
        [
            "openssl",
            "req",
            "-new",
            "-key",
            str(output_dir / f"{name}.key"),
            "-out",
            str(output_dir / f"{name}.csr"),
            "-config",
            str(output_dir / config_name),
        ]
    )
    _run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(output_dir / f"{name}.csr"),
            "-CA",
            str(output_dir / ca_cert),
            "-CAkey",
            str(output_dir / ca_key),
            "-CAcreateserial",
            "-out",
            str(output_dir / f"{name}.crt"),
            "-days",
            "825",
            "-sha256",
            "-extfile",
            str(output_dir / config_name),
            "-extensions",
            "v3_req",
        ]
    )


def _generate_ca(output_dir: Path, *, name: str, config_name: str, config_text: str) -> None:
    _write_text(output_dir / config_name, config_text)
    _run(["openssl", "genrsa", "-out", str(output_dir / f"{name}.key"), "4096"])
    _run(
        [
            "openssl",
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-key",
            str(output_dir / f"{name}.key"),
            "-sha256",
            "-days",
            "3650",
            "-out",
            str(output_dir / f"{name}.crt"),
            "-config",
            str(output_dir / config_name),
        ]
    )


def generate_kafka_assets(output_dir: Path) -> None:
    store_password = os.getenv("KAFKA_CERT_STORE_PASSWORD", "changeit")
    scram_username = os.getenv("KAFKA_SCRAM_USERNAME", "agora")
    scram_password = os.getenv("KAFKA_SCRAM_PASSWORD", "agora-scram-secret")
    schema_registry_username = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USERNAME", "agora")
    schema_registry_password = os.getenv(
        "SCHEMA_REGISTRY_BASIC_AUTH_PASSWORD",
        "agora-registry-secret",
    )
    keytool_bin = _resolve_keytool()

    _reset_output_dir(output_dir)
    _generate_ca(output_dir, name="ca", config_name="ca.cnf", config_text=KAFKA_CA_CONFIG)
    _generate_ca(
        output_dir,
        name="rogue-ca",
        config_name="rogue-ca.cnf",
        config_text=KAFKA_ROGUE_CA_CONFIG,
    )
    _generate_signed_cert(
        output_dir=output_dir,
        name="server",
        config_name="server.cnf",
        config_text=KAFKA_SERVER_CONFIG,
        ca_cert="ca.crt",
        ca_key="ca.key",
    )
    _generate_signed_cert(
        output_dir=output_dir,
        name="client",
        config_name="client.cnf",
        config_text=KAFKA_CLIENT_CONFIG,
        ca_cert="ca.crt",
        ca_key="ca.key",
    )
    _generate_signed_cert(
        output_dir=output_dir,
        name="rogue-client",
        config_name="rogue-client.cnf",
        config_text=KAFKA_ROGUE_CLIENT_CONFIG,
        ca_cert="rogue-ca.crt",
        ca_key="rogue-ca.key",
    )
    _generate_signed_cert(
        output_dir=output_dir,
        name="schema-registry",
        config_name="schema-registry.cnf",
        config_text=KAFKA_SCHEMA_REGISTRY_CONFIG,
        ca_cert="ca.crt",
        ca_key="ca.key",
    )

    _run(
        [
            "openssl",
            "pkcs12",
            "-export",
            "-in",
            str(output_dir / "server.crt"),
            "-inkey",
            str(output_dir / "server.key"),
            "-certfile",
            str(output_dir / "ca.crt"),
            "-name",
            "kafka-broker",
            "-out",
            str(output_dir / "server.keystore.p12"),
            "-passout",
            f"pass:{store_password}",
        ]
    )
    _run(
        [
            "openssl",
            "pkcs12",
            "-export",
            "-in",
            str(output_dir / "client.crt"),
            "-inkey",
            str(output_dir / "client.key"),
            "-certfile",
            str(output_dir / "ca.crt"),
            "-name",
            "agora-client",
            "-out",
            str(output_dir / "client.keystore.p12"),
            "-passout",
            f"pass:{store_password}",
        ]
    )
    for truststore_name in ("server.truststore.p12", "client.truststore.p12"):
        _run(
            [
                keytool_bin,
                "-importcert",
                "-noprompt",
                "-alias",
                "CARoot",
                "-file",
                str(output_dir / "ca.crt"),
                "-keystore",
                str(output_dir / truststore_name),
                "-storetype",
                "PKCS12",
                "-storepass",
                store_password,
            ]
        )

    _write_text(
        output_dir / "server.properties",
        f"""
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
ssl.keystore.password={store_password}
ssl.key.password={store_password}
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/server.truststore.p12
ssl.truststore.password={store_password}
listener.name.ssl.ssl.client.auth=required
listener.name.ssl_internal.ssl.client.auth=required
listener.name.sasl_ssl.ssl.client.auth=none
sasl.enabled.mechanisms=SCRAM-SHA-256
listener.name.sasl_ssl.sasl.enabled.mechanisms=SCRAM-SHA-256
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
""",
    )
    _write_text(
        output_dir / "client-mtls.properties",
        f"""
security.protocol=SSL
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/client.truststore.p12
ssl.truststore.password={store_password}
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/client.keystore.p12
ssl.keystore.password={store_password}
ssl.key.password={store_password}
ssl.endpoint.identification.algorithm=
""",
    )
    _write_secret(output_dir / "scram-password.txt", f"{scram_password}\n")
    _write_text(
        output_dir / "security.env",
        f"""
AGORA_TEST_KAFKA_SCRAM_USERNAME={scram_username}
AGORA_TEST_KAFKA_SCRAM_PASSWORD_FILE={output_dir / "scram-password.txt"}
AGORA_TEST_KAFKA_CA_FILE={output_dir / "ca.crt"}
AGORA_TEST_KAFKA_CLIENT_CERT_FILE={output_dir / "client.crt"}
AGORA_TEST_KAFKA_CLIENT_KEY_FILE={output_dir / "client.key"}
AGORA_TEST_SCHEMA_REGISTRY_URL=https://127.0.0.1:18081
AGORA_TEST_SCHEMA_REGISTRY_MTLS_URL=https://127.0.0.1:18443
AGORA_TEST_SCHEMA_REGISTRY_USERNAME={schema_registry_username}
AGORA_TEST_SCHEMA_REGISTRY_PASSWORD_FILE={output_dir / "schema-registry-password.txt"}
""",
    )
    _write_secret(
        output_dir / "schema-registry-password.txt",
        f"{schema_registry_password}\n",
    )
    _run(
        [
            "htpasswd",
            "-bcB",
            str(output_dir / "schema-registry.htpasswd"),
            schema_registry_username,
            schema_registry_password,
        ]
    )
    _write_text(
        output_dir / "schema-registry-nginx.conf",
        KAFKA_SCHEMA_REGISTRY_NGINX_CONFIG,
    )

    for key_name in (
        "ca.key",
        "rogue-ca.key",
        "server.key",
        "client.key",
        "rogue-client.key",
        "schema-registry.key",
    ):
        (output_dir / key_name).chmod(0o600)

    print(f"Generated Kafka secure assets in {output_dir}")


def generate_redis_assets(output_dir: Path) -> None:
    redis_username = os.getenv("REDIS_SECURE_USERNAME", "agora")
    redis_password = os.getenv("REDIS_SECURE_PASSWORD", "agora-redis-secret")

    _reset_output_dir(output_dir)
    _generate_ca(output_dir, name="ca", config_name="ca.cnf", config_text=REDIS_CA_CONFIG)
    _generate_ca(
        output_dir,
        name="rogue-ca",
        config_name="rogue-ca.cnf",
        config_text=REDIS_ROGUE_CA_CONFIG,
    )
    _generate_signed_cert(
        output_dir=output_dir,
        name="server",
        config_name="server.cnf",
        config_text=REDIS_SERVER_CONFIG,
        ca_cert="ca.crt",
        ca_key="ca.key",
    )
    _write_text(output_dir / "redis.conf", REDIS_SERVER_RUNTIME_CONFIG)
    _write_text(
        output_dir / "users.acl",
        f"""
user default off
user {redis_username} on >{redis_password} ~* +@all
""",
    )
    _write_secret(output_dir / "password.txt", redis_password)

    for key_name in ("ca.key", "rogue-ca.key", "server.key", "password.txt"):
        (output_dir / key_name).chmod(0o600)

    print(f"Generated Redis secure assets in {output_dir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="target", required=True)

    kafka = subparsers.add_parser("kafka-secure", help="Generate Kafka secure topology assets.")
    kafka.add_argument("output_dir", nargs="?", default=str(DEFAULT_KAFKA_OUT_DIR))

    redis = subparsers.add_parser("redis-secure", help="Generate Redis secure topology assets.")
    redis.add_argument("output_dir", nargs="?", default=str(DEFAULT_REDIS_OUT_DIR))

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if args.target == "kafka-secure":
        generate_kafka_assets(output_dir)
        return 0
    if args.target == "redis-secure":
        generate_redis_assets(output_dir)
        return 0
    raise SystemExit(f"Unsupported target: {args.target}")


if __name__ == "__main__":
    raise SystemExit(main())
