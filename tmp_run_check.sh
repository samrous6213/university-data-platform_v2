CLASSPATH=$(/opt/hadoop/bin/hadoop classpath 2>/dev/null)
/usr/local/openjdk-8/bin/jjs -cp "$CLASSPATH" /tmp/check_conf.js 2>&1
