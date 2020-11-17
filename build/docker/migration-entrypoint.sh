#!/usr/bin/dumb-init /bin/sh
set -euo pipefail

run_migration()
{
	if [[ -z "${MIGRATION_CMD}" ]]; then
		echo "No migrations to run"
	else
		echo "Running migrations"
		./migration ${MIGRATION_CMD} -env ${APP_ENV}
	fi
}

run_migration
