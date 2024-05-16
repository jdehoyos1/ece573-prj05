#!/bin/bash

docker build -t ece573-prj05-clients:v1 .
kind load docker-image ece573-prj05-clients:v1
