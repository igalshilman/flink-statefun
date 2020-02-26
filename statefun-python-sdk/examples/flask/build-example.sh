#!/bin/bash

# this part would be removed, once statefun will be released to PyPI
cp ../../dist/statefun-1.1.0-py3-none-any.whl .

# build the flask container
docker build -f Dockerfile.python-worker . -t flask-worker 

# this part would be removed, once statefun will be released to PyPI
rm -f statefun-1.1.0-py3-none-any.whl

# build the statefun Flink image
docker build -f Dockerfile.statefun . -t flask-statefun 

