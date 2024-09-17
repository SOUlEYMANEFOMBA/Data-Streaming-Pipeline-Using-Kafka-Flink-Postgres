FROM bitnami/spark:latest

# Installez les outils nécessaires
USER root
RUN apt-get update && \
    apt-get install -y wget && \
    mkdir -p /usr/lib/jvm && \
    wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.14%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.14_9.tar.gz && \
    tar xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.14_9.tar.gz -C /usr/lib/jvm/ && \
    rm OpenJDK11U-jdk_x64_linux_hotspot_11.0.14_9.tar.gz && \
    ln -s /usr/lib/jvm/jdk-11.0.14+9 /usr/lib/jvm/java-11-openjdk-amd64

# Définir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Configurer le PATH
ENV PATH="${JAVA_HOME}/bin:${PATH}"


