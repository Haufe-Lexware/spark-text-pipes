FROM openjdk:8-jdk

MAINTAINER Nicola Bova <nicola.bova@gmail.com>

ENV ENABLE_INIT_DAEMON true
ENV INIT_DAEMON_BASE_URI http://identifier/init-daemon
ENV INIT_DAEMON_STEP spark_master_init

ENV SPARK_VERSION=2.3.1
ENV HADOOP_VERSION=2.7

ADD https://raw.githubusercontent.com/guilhem/apt-get-install/master/apt-get-install /usr/bin/
RUN chmod +x /usr/bin/apt-get-install

RUN apt-get-install -y curl \
      && chmod +x *.sh \
      && wget http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && cd /

RUN curl http://central.maven.org/maven2/com/github/fommil/netlib/netlib-native_system-linux-x86_64/1.1/netlib-native_system-linux-x86_64-1.1-natives.jar -o /spark/jars/netlib-native_system-linux-x86_64-1.1-natives.jar \
	&& curl http://central.maven.org/maven2/com/github/fommil/netlib/native_system-java/1.1/native_system-java-1.1.jar -o /spark/jars/native_system-java-1.1.jar \ 
	&& curl http://central.maven.org/maven2/com/github/fommil/jniloader/1.1/jniloader-1.1.jar -o /spark/jars/jniloader-1.1.jar

RUN apt-get-install -y python3 python3-setuptools python3-pip libopenblas-base

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED 1
