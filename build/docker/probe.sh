#!/bin/sh
#validating the status code of health check api and returning 1 in case of non 2xx status code
if [ "$(curl -s -o /dev/null -H 'Content-Type: application/json' -d '{"service": ""}' -w '%{http_code}' http://localhost:8080/v1/healthcheck)" != 200 ];
then
  exit 1;
fi
