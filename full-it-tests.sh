#!/bin/bash
echo "clear some-mongo docker container if exists"
docker stop some-mongo
docker rm some-mongo

echo "start clear mongo instance, expose 27017 port"
docker run --name some-mongo -p 27017:27017 -d mongo

echo "set QUASAR_MONGODB_3_2 variable"
export QUASAR_MONGODB_3_2="{\"mongodb\":{\"connectionUri\":\"mongodb://localhost:27017\"}}"

echo "run tests"
./sbt test

echo "clear some-mongo docker container after tests"
docker stop some-mongo
docker rm some-mongo
