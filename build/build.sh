#!/bin/bash

readonly NAME="chronicle"
rm -rf "$NAME".py
rm -rf "$NAME"-0.0.1-py3-none-any.whl

# Assemble notebook from source files.
echo "# Databricks notebook source" > "$NAME".py
declare -i i=1
for file in ../source/*; do
    if [ $i -gt 1 ]; then echo -e "\n" >> "$NAME".py; fi
    cat $file >> "$NAME".py
    i+=1
done

# Use docker to build python wheel.
docker build -t "$NAME" .
docker rmi $(docker images -f dangling=true -q)
docker run --name "$NAME" "$NAME"
docker cp "$NAME":/whl/dist/"$NAME"-0.0.1-py3-none-any.whl .
docker rm "$NAME"

# Move artifacts.
mv -f "$NAME".py ../library
mv -f "$NAME"-0.0.1-py3-none-any.whl ../library
