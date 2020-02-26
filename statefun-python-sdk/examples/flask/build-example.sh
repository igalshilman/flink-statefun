#!/bin/bash

cp ../../dist/statefun-1.1.0-py3-none-any.whl .

docker build -f Dockerfile.python-worker . -t python-statefun-worker

rm -f statefun-1.1.0-py3-none-any.whl

docker build -f Dockerfile.statefun . -t statefun-cluster

