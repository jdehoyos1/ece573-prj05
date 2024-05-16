#!/bin/bash

docker pull confluentinc/cp-zookeeper:7.3.5
docker pull confluentinc/cp-kafka:7.3.5
kind delete cluster
kind create cluster --config cluster.yml
kind load docker-image confluentinc/cp-zookeeper:7.3.5
kind load docker-image confluentinc/cp-kafka:7.3.5
