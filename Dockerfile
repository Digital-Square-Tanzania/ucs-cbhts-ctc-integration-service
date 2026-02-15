FROM gradle:8.5-jdk17 AS build

WORKDIR /workspace

COPY gradlew gradlew.bat build.gradle settings.gradle /workspace/
COPY gradle /workspace/gradle
COPY src /workspace/src
COPY resources /workspace/resources

RUN ./gradlew --no-daemon clean shadowJar

FROM amazoncorretto:17

WORKDIR /app

COPY --from=build /workspace/build/libs/ucs-ctc-integration-service-1.0.0.jar /app/app.jar

EXPOSE 8080

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
