FROM python:3.7-alpine

COPY . /statefun-python-sdk
WORKDIR /statefun-python-sdk
RUN python setup.py sdist bdist_wheel
RUN python setup.py install --user

