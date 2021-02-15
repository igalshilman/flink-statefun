### This is an example of `knaufk` talk


# one time setup
* build statefun from source (from this branch)
* build docker image with `tools/docker/build-stateful-functions.sh`
* then from `knaufk-example/greeter` build the example code (`mvn clean package)`

# from `knaufk-example/greeter`

docker-compose build
docker-compose up
docker-compose down --remove-orphans
