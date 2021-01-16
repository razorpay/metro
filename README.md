# 🚇 metro <!-- omit in toc -->
![build](https://github.com/razorpay/metro/workflows/.github/workflows/build.yml/badge.svg)
<a href="https://somsubhra.com/github-release-stats/?username=razorpay&repository=metro">
  <img alt="Github Releases Stats" src="https://img.shields.io/github/downloads/razorpay/metro/total.svg?logo=github">
</a>
<a href="https://starcharts.herokuapp.com/razorpay/metro"><img alt="Stars" src="https://img.shields.io/github/stars/razorpay/metro.svg?style=social"></a>

Fast and reliable message transport between microservices!

🚧  **Disclaimer**: `metro` is under active development currently. The first release is expected soon. Watch this space for updates.

## What is `metro`?
`metro` is an asynchronous pub-sub messaging platform that decouples producers and consumers of messages and forms the backbone for event-driven applications. `metro` offers durable message storage and real-time message delivery with high availability, fault-tolerance and consistent performance at scale.

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

Before running `metro` components, bring up the dependent datastores
```sh
make dev-docker-datastores-up
```

The datastores can be brough down as well with this command
```sh
make dev-docker-datastores-down
```

Various `metro` components can now be run. `metro` has 4 components, all components are part of same binary and can be ran using a cmd param `component`

Running Metro Web component
```
bin/metro -component web
```

Running Metro Worker component
```
bin/metro -component worker
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
### Running `metro` docker image
The first step is to bring the dependent datastores up (kafka, zookeeper, consul and pulsar)

```sh
make dev-docker-datastores-up
```
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
## Contributing guidelines

### Style checking and unit tests
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

### Integration tests
Integration tests are run as part of continuous integration on github actions. They test end to end integration of various metro components along with datastores.

Integration tests can also be run locally

Before running them locally, make sure metro is running locally. After which, run the following make target
```sh
make test-integration
```

### Compatibility tests
Compatibility tests are also run as part of continuous integration to test compatibility against google pub/sub.

To run them locally, make sure metro is running locally along with google pub/sub emulator.

To bring up the google pub/sub emulator, run the following
```sh
make dev-docker-emulator-up
```
After which, run the following make target
```sh
make test-compat
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


