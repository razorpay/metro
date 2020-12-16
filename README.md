# 🚇 metro <!-- omit in toc -->
Fast and reliable message transport between microservices!

🚧  **Disclaimer**: `metro` is under active development currently. The first release is expected soon. Watch this space for updates.

## What is `metro`?
`metro` is an asynchronous messaging platform that decouples producers and consumers of messages and forms the backbone for event-driven applications. `metro` offers durable message storage and real-time message delivery with high availability, fault-tolerance and consistent performance at scale.

## Local setup
Clone the repo with submodules
```sh
git clone --recurse-submodules -j8 https://github.com/razorpay/metro
cd metro
```
### Building the binary
Install dependencies
```sh
make deps
```
Generate go bindings from proto definitions
```sh
make proto-generate
```
Build the binary
```sh
make go-build-metro
```
The binary can be found at `bin/` directory
### Running the binary
metro has 4 components, all components are part of same binary and can be ran using a cmd param `component`

Running Metro Producer
```
bin/metro -component producer
```

Running Metro Pull Consumer
```
bin/metro -component pull-consumer
```

Running Metro Push Consumer
```
bin/metro -component push-consumer
```

Running OpenAPI Server
```
bin/metro -component openapi-server
```

### Building the `metro` docker image
After cloning the repository, run
```sh
make docker-build-metro
```
### Running it with `docker-compose`
The `metro` image along with the monitoring stack, prometheus and jaeger, can be run as
```sh
make dev-docker-up
```
To bring the setup down
```sh
 make dev-docker-down
 ```
To rebuild
 ```sh
 make dev-docker-rebuild
 ```
### Contributing guidelines
`metro` runs unit tests, `golint` and `goimports` check in the continuous integration (CI) pipeline.

You can locally test these changes by running the following `make` targets

`golint` check
```sh
make lint-check
```

`goimports` check
```sh
make goimports-check
```

You can also apply `goimports` changes locally by doing the following
```sh
make goimports
```

Running unit tests
```sh
make test-unit
```
### Updating the `metro-proto` submodule
To update the submodule to the latest remote commit
```sh
git submodule update --remote --merge
```
 ## Accessing the APIs
 The default ports for various services in `metro`
* gRPC endpoint on port 8081
* `grpc-gateway` HTTP endpoint on port 8082
* swagger docs on port 3000


