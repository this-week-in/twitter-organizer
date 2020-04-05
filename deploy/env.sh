#!/usr/bin/env bash

source "$(cd $(dirname $0) && pwd)/env.sh"

mkdir -p $HOME/bin
export PATH=$PATH:$HOME/bin
