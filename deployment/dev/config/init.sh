#!/bin/bash
while ! curl -f -s http://$CONSUL_HTTP_ADDR/v1/status/leader | grep "[0-9]:[0-9]"; do
  sleep 1
done
consul kv put metro-dev/projects/sakshi-project001 {\"name\":\"sakshi-project001\",\"project_id\":\"sakshi-project001\",\"labels\":null}
consul kv put metro-dev/credentials/sakshi-project001/sakshi-project001__a13a5a {\"username\":\"sakshi-project001__a13a5a\",\"password\":\"f8e1c8f501ce550779c1d7a12d18148bbf076d8300393e0d561eb4e9d0cfc3aafdcf2348a25ed92a2bd4e1bf9cf16511\",\"project_id\":\"sakshi-project001\"}
