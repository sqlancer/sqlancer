FROM ubuntu:21.04

RUN apt-get update --yes && env DEBIAN_FRONTEND=noninteractive apt-get install openjdk-15-jdk maven --yes --no-install-recommends

# assumes that the project has already been built
COPY target/sqlancer-*.jar sqlancer.jar
COPY target/lib/*.jar /lib/

ENTRYPOINT ["java", "-jar", "sqlancer.jar"]
