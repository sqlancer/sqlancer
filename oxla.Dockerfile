FROM maven:3.9.9-amazoncorretto-21-debian

RUN echo "Building SQLancer JAR file"
ADD . .
RUN mvn package -DskipTests -X

RUN echo "Running SQLancer"
WORKDIR /target
ENTRYPOINT ["java", "-jar", "sqlancer-2.0.0.jar"]
