#!/usr/bin/dumb-init /bin/sh
set -euo pipefail

initialize()
{
    echo "Initializing metro"

    if [ "$APP_ENV" == "dev_docker" ]
    then
      return
    fi

    mkdir -p /app/configs
    touch /app/configs/ca-cert.pem
    touch /app/configs/user-cert.pem
    touch /app/configs/user.key

    printf '%s\n' "$KAFKA_CA_CERT" | sed 's/- /-\n/g; s/ -/\n-/g' | sed '/CERTIFICATE/! s/ /\n/g' > /app/configs/ca-cert.pem
    printf '%s\n' "$KAFKA_USER_CERT" | sed 's/- /-\n/g; s/ -/\n-/g' | sed '/CERTIFICATE/! s/ /\n/g' > /app/configs/user-cert.pem
    printf '%s\n' "$KAFKA_USER_KEY" | sed 's/- /-\n/g; s/ -/\n-/g' | sed '/PRIVATE/! s/ /\n/g' > /app/configs/user.key
}

start_application()
{
    su-exec appuser $WORKDIR/"$appName" -component=${COMPONENT} &

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
initialize
start_application

