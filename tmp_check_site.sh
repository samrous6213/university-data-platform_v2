#!/bin/sh
for jar in /opt/hadoop/share/hadoop/common/*.jar; do
  if [ -f "$jar" ]; then
    found=$(grep -lc "core-site.xml" "$jar" 2>/dev/null)
    if [ "$found" -gt 0 ] 2>/dev/null; then
      echo "Found core-site.xml in $jar"
    fi
  fi
done
for jar in /opt/hive/lib/*.jar; do
  if [ -f "$jar" ]; then
    found=$(grep -lc "core-site.xml" "$jar" 2>/dev/null)
    if [ "$found" -gt 0 ] 2>/dev/null; then
      echo "Found core-site.xml in $jar"
    fi
  fi
done
echo "DONE"
