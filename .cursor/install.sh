#!/usr/bin/env bash
# Idempotent Cloud Agent bootstrap for the Atlantis repository.
# Refreshes Go and website dependencies and installs the Terraform and
# conftest binaries required by the integration/e2e test suites.
set -euo pipefail

# Versions pinned to match the production Dockerfile.
TERRAFORM_VERSION="1.14.9"
CONFTEST_VERSION="0.66.0"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==> Downloading Go modules"
go mod download

echo "==> Installing website (VitePress) dependencies"
npm ci --no-audit --no-fund

install_terraform() {
  if command -v terraform >/dev/null 2>&1 &&
     terraform version 2>/dev/null | head -n1 | grep -q "v${TERRAFORM_VERSION}"; then
    echo "==> Terraform ${TERRAFORM_VERSION} already installed"
    return
  fi
  echo "==> Installing Terraform ${TERRAFORM_VERSION}"
  local tmp
  tmp="$(mktemp -d)"
  (
    cd "$tmp"
    curl -fsSLO "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip"
    curl -fsSLO "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_SHA256SUMS"
    sed -n "/terraform_${TERRAFORM_VERSION}_linux_amd64.zip/p" "terraform_${TERRAFORM_VERSION}_SHA256SUMS" | sha256sum -c
    sudo mkdir -p "/opt/terraform/${TERRAFORM_VERSION}"
    sudo unzip -o "terraform_${TERRAFORM_VERSION}_linux_amd64.zip" -d "/opt/terraform/${TERRAFORM_VERSION}"
    sudo ln -sf "/opt/terraform/${TERRAFORM_VERSION}/terraform" /usr/local/bin/terraform
    sudo ln -sf "/opt/terraform/${TERRAFORM_VERSION}/terraform" "/usr/local/bin/terraform${TERRAFORM_VERSION}"
  )
  rm -rf "$tmp"
}

install_conftest() {
  if command -v conftest >/dev/null 2>&1 &&
     conftest --version 2>/dev/null | head -n1 | grep -q "${CONFTEST_VERSION}"; then
    echo "==> conftest ${CONFTEST_VERSION} already installed"
    return
  fi
  echo "==> Installing conftest ${CONFTEST_VERSION}"
  local tmp
  tmp="$(mktemp -d)"
  (
    cd "$tmp"
    curl -fsSLO "https://github.com/open-policy-agent/conftest/releases/download/v${CONFTEST_VERSION}/conftest_${CONFTEST_VERSION}_Linux_x86_64.tar.gz"
    curl -fsSLO "https://github.com/open-policy-agent/conftest/releases/download/v${CONFTEST_VERSION}/checksums.txt"
    sed -n "/conftest_${CONFTEST_VERSION}_Linux_x86_64.tar.gz/p" checksums.txt | sha256sum -c
    sudo mkdir -p "/usr/local/bin/cft/versions/${CONFTEST_VERSION}"
    sudo tar -C "/usr/local/bin/cft/versions/${CONFTEST_VERSION}" -xzf "conftest_${CONFTEST_VERSION}_Linux_x86_64.tar.gz"
    sudo ln -sf "/usr/local/bin/cft/versions/${CONFTEST_VERSION}/conftest" /usr/local/bin/conftest
  )
  rm -rf "$tmp"
}

install_terraform
install_conftest

echo "==> Install complete"
echo "    go:        $(go version)"
echo "    node:      $(node --version)"
echo "    terraform: $(terraform version | head -n1)"
echo "    conftest:  $(conftest --version | head -n1)"
