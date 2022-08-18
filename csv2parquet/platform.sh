#!/usr/bin/env bash

# Used in Docker build to set platform dependent variables. Adapted from:
#
# * https://blog.container-solutions.com/building-multiplatform-container-images

case $TARGETARCH in
    "amd64")
	echo "x86_64-unknown-linux-gnu" > /.platform
	echo "gcc-x86-64-linux-gnu" > /.compiler
	;;
    "arm64")
	echo "aarch64-unknown-linux-gnu" > /.platform
	echo "gcc-aarch64-linux-gnu" > /.compiler
	;;
esac