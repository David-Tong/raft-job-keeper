FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
COPY job-keeper-common/pom.xml job-keeper-common/
COPY job-keeper-server/pom.xml job-keeper-server/
COPY job-keeper-worker/pom.xml job-keeper-worker/
RUN mvn -B -q -pl job-keeper-server -am dependency:go-offline
COPY . .
RUN mvn -B -q -pl job-keeper-server -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache curl tzdata \
 && addgroup -S -g 10001 app && adduser -S -u 10001 -G app app \
 && mkdir -p /data && chown app:app /data
COPY --from=build /src/job-keeper-server/target/job-keeper-server.jar /app/job-keeper-server.jar
USER 10001
VOLUME ["/data"]
EXPOSE 7000 8080 9090
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError --add-opens=java.base/java.util=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/job-keeper-server.jar \"$@\"", "--"]
CMD ["--raft.data-dir=/data", "--raft.port=7000", "--server.port=8080", "--grpc.port=9090"]
