#!/usr/bin/env bash
#

set -euo pipefail

[[ $# -gt 0 ]] || set -- test

containerName="quasar-test-mongo"

if docker stop $containerName &>/dev/null && docker rm $containerName &>/dev/null; then
  echo >&2 "Removed existing $containerName container"
fi

dockerPid="$(docker run --name $containerName -p 27017:27017 -d mongo)"

cleanup () {
  docker rm -f $containerName &>/dev/null
}
trap cleanup EXIT

set -x
export QUASAR_MONGODB_3_2="{\"mongodb\":{\"connectionUri\":\"mongodb://localhost:27017\"}}"
./bin/sbt-travis.sh "$@"
