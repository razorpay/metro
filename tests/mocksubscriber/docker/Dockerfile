#build stage
FROM golang:1.16.5-alpine3.13 as builder

ENV CGO_ENABLED 1
ARG GIT_COMMIT_HASH
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}
ENV SRC_DIR=/src

ADD . $SRC_DIR/

WORKDIR $SRC_DIR

RUN go build -o mock-server tests/mocksubscriber/cmd/main.go

# final stage
FROM alpine:3.13

COPY --from=builder /src/mock-server /app/
COPY ./tests/mocksubscriber/docker/entrypoint.sh /app/

ENV WORKDIR=/app
ENV DUMB_INIT_SETSID=0
ARG GIT_COMMIT_HASH
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

WORKDIR /app

RUN apk add --update --no-cache dumb-init su-exec curl
RUN mkdir -p /app/public && \
    echo "${GIT_COMMIT_HASH}" > /app/public/commit.txt

EXPOSE 8099

RUN chmod +x entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh", "mock-server"]
