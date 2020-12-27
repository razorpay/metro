#!/usr/bin/dumb-init /bin/sh
set -euo pipefail

start_application()
{
    if [[ -n "${GIT_COMMIT_HASH}" ]]; then
      echo "${GIT_COMMIT_HASH}" > /app/public/commit.txt
    fi
    echo "su-exec appuser $WORKDIR/"$appName" -component=${METRO_COMPONENT} &"
    su-exec appuser $WORKDIR/"$appName" -component=${METRO_COMPONENT} &

    # Get pid for app
    APP_PID=$!

    # wait returns immediately after signal is caught,
    # hence double wait is required in shutdown_application
    # refer : http://veithen.io/2014/11/16/sigterm-propagation.html
    wait "$APP_PID"
}

shutdown_application()
{
    kill -s SIGTERM "$APP_PID"
    trap - SIGTERM SIGINT
    wait "$APP_PID"
    EXIT_STATUS=$?
    return ${EXIT_STATUS}
}

appName="$1"
trap shutdown_application SIGTERM SIGINT
start_application

