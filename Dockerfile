FROM gradle:6.7.0-jdk15 AS build

COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src

RUN gradle build --no-daemon

FROM openjdk:15.0.1
LABEL author="Michael Wilson"
LABEL repository="http://github.com/mewil/sturgeon"

RUN mkdir /app
COPY --from=build /home/gradle/src/build/libs/*.jar /app/sturgeon.jar

ENTRYPOINT [ "java", "-jar", "/app/sturgeon.jar" ]
EXPOSE 8080