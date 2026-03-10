FROM apache/hive:4.0.1

ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_VERSION=1.12.787

USER root

RUN set -x && \
    apt-get update && \
    apt-get install -y wget && \
    \
    wget -P /opt/hive/lib/ "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    \
    wget -P /opt/hive/lib/ "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    \
    apt-get purge -y --auto-remove wget && \
    rm -rf /var/lib/apt/lists/*

USER hive