#!/bin/bash

# Create version related files that can be embedded in SRPM where we do not
# have access to git when running e.g. "make build".
#
# VERSION: string used in the logs from edm
#
# RPM_VERSION: string used in RPM package names
# The string contains a snapshot as described here:
# https://docs.fedoraproject.org/en-US/packaging-guidelines/Versioning/#_snapshots
#
# Run shellcheck and shfmt on this file prior to commiting.

if current_tag=$(git describe --exact-match --tags 2>/dev/null); then
	printf '%s\n' "$current_tag" >VERSION
	printf '%s\n' "${current_tag/#v/}" >RPM_VERSION
else
	git rev-parse HEAD >VERSION

	if last_tag=$(git describe --tags --abbrev=0 2>/dev/null); then
		rpm_base_version="${last_tag/#v/}"
	else
		rpm_base_version="0.0.0"
	fi

	short_sha=$(git rev-parse --short HEAD)
	date=$(date +%Y%m%d)
	printf '%s\n' "$rpm_base_version^$date.$short_sha" >RPM_VERSION
fi
