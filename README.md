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
 ## Accessing the APIs
 `metro` exposes a 
* gRPC endpoint on port 8081.
* HTTP endpoint on port 8082
* Swagger docs on port 3000


