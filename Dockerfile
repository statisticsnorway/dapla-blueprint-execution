#
# Build Application image
#
FROM openjdk:15-slim

#
# Resources from build image
#
#COPY --from=build /linked /jdk/
COPY target/libs /app/lib/
COPY target/blueprint-execution*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/

#ENV PATH=/jdk/bin:$PATH

WORKDIR /app

CMD ["java", "-cp", "/app/lib/*", "no.ssb.dapla.blueprintexecution.BlueprintExecutionApplication"]

EXPOSE 10180
