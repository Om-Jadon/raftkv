#!/bin/sh
set -e
mkdir -p /data
# Named volumes are created as root; the server user must own the dir.
if [ "$(id -u)" = 0 ]; then
	chown -R raftkv:raftkv /data
	exec su-exec raftkv /usr/local/bin/raftkvd "$@"
fi
exec /usr/local/bin/raftkvd "$@"
