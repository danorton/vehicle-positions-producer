# Below is instead of unavailable "FROM openjdk:11-alpine"
FROM alpine:3.11.3
ENV JAVA_HOME="/usr/lib/jvm/default-jvm/"
RUN apk add --no-cache "openjdk11-jre-headless=11.0.5_p10-r0"
ENV PATH=$PATH:${JAVA_HOME}/bin
ENV TRANSIT_VP_STALE_AGE_MINUTES=5
ENV TRANSIT_VP_MAX_WAIT_TIME_SECONDS=10
ENV TRANSIT_VP_VEHICLE_POSITIONS_PB_URL="https://data.texas.gov/download/eiei-9rpf/application%2Foctet-stream"
ENV TRANSIT_VP_REDIS_HOST="redis:6379"
ENV TRANSIT_VP_UNCHANGED_SLEEP_SECONDS=2
ENV TRANSIT_VP_CHANGED_SLEEP_SECONDS=9
WORKDIR .
COPY target/vehiclepositions-producer-1.0-SNAPSHOT-jar-with-dependencies.jar vehicle-positions-producer.jar
COPY run-stuff.sh .
CMD sh run-stuff.sh
# CMD ["java","-jar","vehicle-positions-producer.jar"]
