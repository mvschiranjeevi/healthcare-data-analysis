#!/usr/bin/env bash

# run Localstack
aws --endpoint-url=http://localhost:4566 s3 mb s3://raw-bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://cleaned-bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://curated-bucket
