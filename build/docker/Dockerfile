# Stage 1 - Generate proto
###########################
FROM golang:1.19.0-alpine3.16 as proto-builder

ENV CGO_ENABLED 1
ENV METRO_APPENV=${APP_ENV}
ARG GIT_COMMIT_HASH
ENV METRO_APP_GITCOMMITHASH=${GIT_COMMIT_HASH}

RUN apk add --update --no-cache --repository https://dl-4.alpinelinux.org/alpine/latest-stable/community/ build-base gcc make git librdkafka-dev pkgconf curl

RUN mkdir -p /src/scripts
WORKDIR /src

# App source code is not required for this stage, only add what's needed
ADD Makefile /src
ADD metro-proto /src/metro-proto
ADD buf.gen.yaml /src/
ADD buf.yaml /src/
ADD third_party /src/third_party
ADD go.mod /src/
ADD go.sum /src/

RUN make deps
RUN make proto-generate


# Stage 2 - Compilation build stage
######################################
FROM golang:1.19.0-alpine3.16 as builder

ENV CGO_ENABLED 1
ENV METRO_APPENV=${APP_ENV}
ARG GIT_COMMIT_HASH
ENV METRO_APP_GITCOMMITHASH=${GIT_COMMIT_HASH}

RUN mkdir /src
WORKDIR /src

RUN apk add --update --no-cache --repository https://dl-4.alpinelinux.org/alpine/latest-stable/community/ build-base gcc make git librdkafka-dev pkgconf curl

# Fetch proto from previous stage
COPY --from=proto-builder /src/rpc /src/rpc
COPY --from=proto-builder /src/statik /src/statik


# Avoid copying the whole source code first since that will invalidate
# cache for all further layers
COPY go.mod .
COPY go.sum .

# Copy rest of the source code
ADD . /src/

RUN make deps
RUN make mock-gen
RUN go mod vendor
RUN make go-build-metro

# Stage 3 - Binary build stage
######################################
FROM alpine:3.16

COPY --from=builder /src/bin/metro /app/
COPY --from=builder /src/config/ /app/config/
COPY build/docker/probe.sh /app/
COPY build/docker/entrypoint.sh /app/

ENV WORKDIR=/app
ENV DUMB_INIT_SETSID=0
ENV METRO_APPENV=${APP_ENV}
ARG GIT_COMMIT_HASH
ENV METRO_APP_GITCOMMITHASH=${GIT_COMMIT_HASH}
ENV COMPONENT=${COMPONENT}

WORKDIR /app

RUN apk add --update --no-cache dumb-init su-exec ca-certificates curl
RUN mkdir -p /app/public

EXPOSE 8081 8082 8083 8084 3000

RUN chmod +x entrypoint.sh probe.sh
ENTRYPOINT ["/app/entrypoint.sh", "metro"]
