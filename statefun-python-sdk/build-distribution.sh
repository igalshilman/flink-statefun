#!/bin/sh

rm -fr dist

docker run \
	-v "$(pwd):/app" \
	--workdir /app \
	-i  python:3.7-alpine \
	python3 setup.py sdist bdist_wheel

rm -fr statefun.egg-info
rm -fr build


