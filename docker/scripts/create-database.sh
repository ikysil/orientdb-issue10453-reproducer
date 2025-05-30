#!/usr/bin/env bash

# override - no need to debug this
JAVA_OPTS=""

cat <<-EOF | ./console.sh
CONNECT ENV embedded:/orientdb/databases root root
CREATE DATABASE Issue10453 plocal
EXIT
EOF
