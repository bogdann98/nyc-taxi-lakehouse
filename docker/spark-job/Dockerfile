FROM apache/spark:3.5.3

USER root
WORKDIR /opt/spark/work-dir

# Pinned version matrix — see README "Trade-offs"
ENV ICEBERG_VERSION=1.6.1
ENV NESSIE_VERSION=0.99.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV SPARK_VERSION=3.5.3
ENV KAFKA_CLIENTS_VERSION=3.4.1

# Pre-bake jars into $SPARK_HOME/jars/ so spark-submit doesn't need internet
# at run time (vs --packages which would re-download via Ivy on every start).
RUN cd /opt/spark/jars && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY streaming_job.py /opt/spark/work-dir/streaming_job.py
COPY entrypoint.sh    /opt/spark/work-dir/entrypoint.sh
RUN chmod +x /opt/spark/work-dir/entrypoint.sh

USER 185
ENTRYPOINT ["/opt/spark/work-dir/entrypoint.sh"]
