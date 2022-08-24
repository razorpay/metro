FROM consul:1.9.0

USER root
COPY ./config /consul/config
RUN chmod +x /consul/config/init.sh
RUN sed -ie 's|^\(set .*\)|\1\nsh /consul/config/init.sh \&|' /usr/local/bin/docker-entrypoint.sh
