#
# Build Application image
#
FROM openjdk:14-slim

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

CMD ["java", "-p", "/app/lib", "-m", \
"no.ssb.dapla.blueprintexecution/no.ssb.dapla.blueprintexecution.BlueprintExecutionApplication"]

EXPOSE 10180
