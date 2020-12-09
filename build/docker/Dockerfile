# Stage 1 - Generate proto
###########################
FROM golang:1.15.5-alpine3.12 as proto-builder

ARG GIT_TOKEN
ENV CGO_ENABLED 1

RUN apk add --update --no-cache --repository https://dl-4.alpinelinux.org/alpine/latest-stable/community/ build-base gcc make git librdkafka-dev pkgconf

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
FROM golang:1.15.5-alpine3.12 as builder

ARG GIT_TOKEN
ENV CGO_ENABLED 1

RUN mkdir /src
WORKDIR /src

RUN apk add --update --no-cache --repository https://dl-4.alpinelinux.org/alpine/latest-stable/community/ build-base gcc make git librdkafka-dev pkgconf

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
FROM alpine:3.12
ARG GIT_COMMIT_HASH
ARG SERVICE

COPY --from=builder /src/bin/metro /app/
COPY --from=builder /src/config/ /app/config/
COPY build/docker/probe.sh /app/
COPY build/docker/entrypoint.sh /app/

ENV WORKDIR=/app
ENV DUMB_INIT_SETSID=0
WORKDIR /app

RUN apk add --update --no-cache dumb-init su-exec ca-certificates curl
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}
RUN mkdir -p /app/public && \
    echo "$GIT_COMMIT_HASH" > /app/public/commit.txt

ENV SERVICE=${SERVICE}

EXPOSE 8081 8084 8087

RUN chmod +x entrypoint.sh probe.sh
ENTRYPOINT ["/app/entrypoint.sh", "metro"]
