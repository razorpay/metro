# metro

go-foundation is the boilerplate for go microservices. Anyone trying to build a microservice from scratch,
can use this boilerplate to get started with codebase.
Go-foundation handles all the configuration of dependency, docker, other config in toml and is ready to be deployed with
basic structure.

## Documentation
* 1
* 2
* godoc link

[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=razorpay/$app_name&identifier=157173150)](https://dependabot.com)
![build status](https://drone.razorpay.com/api/badges/razorpay/$app_name/status.svg "Drone Build Status")

## Components
-  bin folder contains the build project file. This folder should be git ignore.
-  docker folder contains the Dockerfile for different services. It contains the commands that needs to be executed while building the project.
-  build folder also contain the entrypoint file which get executed on start of application.
-  cmd folder contains the main application file for api , worker and migration.
-  config file contains the all the toml configuration thats required to define the config which gets parsed in Config struct.
- deployment folder contains the dev docker compose file if you want to run the application using docker for dev.
- internal folder is the place where your code goes for you application.
- pkg contains the required dependencies for boilerplate and utilities like prometheus.


## Changes Required

- 1 Need to change the appName in MakeFile, Dockerfile.api, Dockerfile.migration, Dockerfile.worker and Dockerfile.base.
- 2 Change toml file based on the requirement of your service.

#### ENV vars expected
1. APP_ENV

## Auxillary Components

## Building locally
Change the docker_dev toml file for hostname , service name and run command
docker-compose build
## Running locally
docker-compose -f docker-compose.yml up -d --build
### In docker environment
