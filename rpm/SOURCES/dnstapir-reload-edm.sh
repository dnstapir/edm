#!/usr/bin/env bash

# bash settings
set -o errexit
set -o nounset
set -o pipefail

# variables
SERVICE="dnstapir-edm.service"

# request pid for the EDM service
PID=$(systemctl show --property MainPID --value "${SERVICE}")
if [ "${PID}" -eq "0" ]; then
  printf "Could not find a running instance of systemd service '%s'\n" "${SERVICE}" 1>&2
  if systemctl is-active --quiet "${SERVICE}"; then
    printf "...but the service is active!\n" 1>&2
    exit 1
  fi
  exit 0
fi

# attempt to send a SIGHUP to the EDM service
if ! kill -HUP "${PID}"; then
    printf "Could not send SIGHUP to systemd service '%s'\n" "${SERVICE}" 1>&2
    exit 2
fi

printf "Successfully sent a SIGHUP to systemd service '%s'\n" "${SERVICE}"
exit 0
