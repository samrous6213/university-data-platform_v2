#!/bin/sh
CLASSPATH=$(cat /proc/1/environ | tr '\0' '\n' | grep '^CLASSPATH=' | sed 's/^CLASSPATH=//')
/usr/local/openjdk-8/bin/jjs -cp "$CLASSPATH" /tmp/check_config.js 2>&1 | head -20
