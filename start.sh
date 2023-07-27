#!/bin/sh
docker rm --force vent-simulator
docker build -t vent-simulator:latest .
docker run -d --name vent-simulator vent-simulator:latest