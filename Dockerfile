FROM ubuntu:21.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-15-jdk \
    maven && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m sqlancer && \
    chown -R sqlancer:sqlancer /home/sqlancer

# Copy the project files
WORKDIR /home/sqlancer
COPY target/sqlancer-*.jar sqlancer.jar
COPY target/lib/*.jar /lib/
RUN chown -R sqlancer:sqlancer /home/sqlancer

USER sqlancer

ENTRYPOINT ["java", "-jar", "sqlancer.jar"]
