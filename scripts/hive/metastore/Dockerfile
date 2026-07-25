FROM apache/hive:3.1.3

USER root

RUN apt-get update && apt-get install -y curl netcat-openbsd gettext-base && \
    curl -fsSL -o /opt/hive/lib/postgresql-42.7.1.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar && \
    curl -fsSL -o /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.1.0.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar && \
    curl -fsSL -o /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.11.199.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.199/aws-java-sdk-bundle-1.11.199.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && chown -R hive:hive /opt/hive/conf

USER hive

EXPOSE 9083

ENTRYPOINT ["/entrypoint.sh"]
