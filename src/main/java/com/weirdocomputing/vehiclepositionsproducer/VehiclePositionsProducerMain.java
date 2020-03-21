package com.weirdocomputing.vehiclepositionsproducer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.transit.realtime.GtfsRealtime;
import com.weirdocomputing.transitlib.VehiclePosition;
import com.weirdocomputing.transitlib.VehiclePositionCollection;
import io.lettuce.core.RedisClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashSet;
import java.util.Set;

/**
 * Main class for execution. This includes the CLI entry point and the
 * work function when called from other environments (e.g. Lambda)
 */
public class VehiclePositionsProducerMain {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerMain.class);
    private static final Encoder b64Encoder = Base64.getEncoder();
    private static final String DEFAULT_REDIS_HOST = "localhost:6379";
    private static final Duration DEFAULT_STALE_AGE = Duration.ofMinutes(60);
    private static final Instant DEFAULT_GIVE_UP_TIME = Instant.now().plus(Duration.ofSeconds(10));
    private static final Duration DEFAULT_CHANGED_SLEEP_DURATION = Duration.ofSeconds(9);
    private static final Duration DEFAULT_UNCHANGED_SLEEP_DURATION = Duration.ofSeconds(2);
    private static final Duration DEFAULT_TRANSIT_VP_REINDEX_PERIOD_SECONDS = Duration.ofMinutes(2);

    private static String redisHost = DEFAULT_REDIS_HOST;
    private static URL sqsOutputQueueUrl = null;
    private static Duration staleAge = DEFAULT_STALE_AGE;
    private static Instant giveUpTime = DEFAULT_GIVE_UP_TIME;
    private static Duration changedSleepDuration = DEFAULT_CHANGED_SLEEP_DURATION;
    private static Duration unchangedSleepDuration = DEFAULT_UNCHANGED_SLEEP_DURATION;
    private static Duration reindexPeriod = DEFAULT_TRANSIT_VP_REINDEX_PERIOD_SECONDS;

    private static AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withRegion("us-east-2").build();
    // FIXME - bug in AWS library causes region to b
    //    private static AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withRegion("us-east-2").build();

    private static URL protobufUrl;

    static {
        // Read settings from environment variables
        String envValue = System.getenv("TRANSIT_VP_VEHICLE_POSITIONS_PB_URL");
        logger.info("PB_URL=\"{}\"", envValue);
        try {
            protobufUrl = new URL(envValue);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        envValue = System.getenv("TRANSIT_VP_SQS_OUTPUT_URL");
        logger.info("VP_SQS_OUTPUT_URL=\"{}\"", envValue);
        if (envValue != null && !envValue.isBlank()) {
            try {
                sqsOutputQueueUrl = new URL(envValue);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        envValue = System.getenv("TRANSIT_VP_MAX_WAIT_TIME_SECONDS");
        logger.info("MAX_WAIT_TIME_SECONDS={}", envValue);
        if (envValue != null && !envValue.isBlank()) {
            giveUpTime = Instant.now().plus(Duration.ofSeconds(Integer.parseUnsignedInt(envValue)));
        }
        envValue = System.getenv("TRANSIT_VP_CHANGED_SLEEP_SECONDS");
        logger.info("CHANGED_SLEEP_SECONDS={}", envValue);
        if (envValue != null && !envValue.isBlank()) {
            changedSleepDuration = Duration.ofSeconds(Integer.parseUnsignedInt(envValue));
        }
        envValue = System.getenv("TRANSIT_VP_UNCHANGED_SLEEP_SECONDS");
        logger.info("UNCHANGED_SLEEP_SECONDS={}", envValue);
        if (envValue != null && !envValue.isBlank()) {
            unchangedSleepDuration = Duration.ofSeconds(Integer.parseUnsignedInt(envValue));
        }
        envValue = System.getenv("TRANSIT_VP_REINDEX_PERIOD_SECONDS");
        logger.info("REINDEX_PERIOD_SECONDS={}", envValue);
        if (envValue != null && !envValue.isBlank()) {
            reindexPeriod = Duration.ofSeconds(Integer.parseUnsignedInt(envValue));
        }
        envValue = System.getenv("TRANSIT_VP_REDIS_HOST");
        logger.info("REDIS_HOST={}", envValue);
        if (envValue == null || envValue.isBlank()) {
            logger.info("MAX_WAIT_TIME_SECONDS={}", envValue);
        } else {
            redisHost = envValue;
        }
        envValue = System.getenv("TRANSIT_VP_STALE_AGE_MINUTES");
        logger.info("STALE_AGE_MINUTES={}", envValue);
        if (envValue != null && !envValue.isBlank()) {
            staleAge = Duration.ofMinutes(Integer.parseUnsignedInt(envValue));
        }

    }

    /**
     * CLI entry point
     * @param args We don't currently reference the passed arguments
     */
    public static void main(String[] args) {

        VehiclePositionCollection newPositions;

        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                newPositions = fetchUpdates();

                if (newPositions != null) {

                    if (logger.isTraceEnabled()) {
                        logger.trace("{} new positions", newPositions.size());
                        for (VehiclePosition p: newPositions.values()) {
                            logger.trace("Position key: \"{}\"", p.getHashString());
                            logger.trace("Position: {}", p.toJsonNode().toString());
                        }
                    }
                } else {
                    logger.info("No new positions");
                }
            } catch (Exception e) {
                while (e.getMessage() == null) {
                    Exception cause = new Exception(e.getCause());
                    if (cause.getMessage() == null) {
                        break;
                    }
                    e = cause;
                }
                throw new RuntimeException(e);
            }
            if (newPositions != null && newPositions.size() > 0) {
                VehiclePositionCollection positions = new VehiclePositionCollection(
                        Duration.ofMinutes(60),
                        newPositions.values().toArray(new VehiclePosition[0]));
                GtfsRealtime.FeedMessage feedMessage = positions.toFeedMessage(true);
                logger.info("FeedMessage: {} entities {} bytes",
                        feedMessage.getEntityCount(),feedMessage.getSerializedSize());

                // Send SQS message
                SendMessageRequest sendMessageRequest = new SendMessageRequest()
                        .withQueueUrl(sqsOutputQueueUrl.toString())
                        .withMessageBody(b64Encoder.encodeToString(feedMessage.toByteArray()));
                SendMessageResult messageResult = sqsClient.sendMessage(sendMessageRequest);
                logger.info("SQS seq {} id {}", messageResult.getSequenceNumber(), messageResult.getMessageId());
            }
            try {
                long sleepMillis = (newPositions != null && newPositions.size() > 0)
                        ? changedSleepDuration.toMillis()
                        : unchangedSleepDuration.toMillis() ;
                logger.info("Sleeping {} seconds", sleepMillis/1000);
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // ignore interrupt
            }

        }
    }

    private static Instant nextReindexTime = null;
    /**
     * Main processing used by main and Lambda entry points
     * @return collection of vehicle positions that have changed since
     * the last report.
     * @throws Exception Various failures, e.g. server request failures
     */
    @Nullable
    static VehiclePositionCollection fetchUpdates() throws Exception {
        VehiclePositionCollection positionCollection;
        String prevETag, currETag;
        Set<String> latestKeys;

        RedisClient redisClient = RedisClient.create(String.format("redis://%s", redisHost));

        VehiclePositionsIndex oldVpIndex;
        Instant now = Instant.now();
        if (nextReindexTime != null && nextReindexTime.isAfter(now)) {
            oldVpIndex = VehiclePositionsIndex.fromRedis(redisClient);
        } else {
            oldVpIndex = null;
            nextReindexTime = now.plus(reindexPeriod);
        }
        if (oldVpIndex == null) {
            // create an ETag that won't match anything
            prevETag = null;
            latestKeys = new HashSet<>();
            logger.info("##### No previous ETag or reindex timer expired");
        } else {
            prevETag = oldVpIndex.getId();
            latestKeys = oldVpIndex.getKeys();
            logger.info("Previous ETag: {}", prevETag);
        }

        // Get current ETag from publisher
        logger.info("Get VP connection to {}", protobufUrl);

        HttpURLConnection vpConnection = getHttpURLConnection(protobufUrl, prevETag);
        currETag = vpConnection.getHeaderField("ETag");
        logger.info("Connection response code {} ETag {}", vpConnection.getResponseCode(), currETag);

        // If nothing changed, re-read until there's a change
        while((vpConnection.getResponseCode() / 100) != 2 && Instant.now().isBefore(giveUpTime)) {
            // disconnect without reading positions
            vpConnection.disconnect();
            // sleep 1 second between retries
            logger.info("Sleeping 1 second");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Ignore interrupted exception
            }
            logger.info("Get VP connection");
            vpConnection = getHttpURLConnection(protobufUrl, prevETag);
            currETag = vpConnection.getHeaderField("ETag");
            logger.info("Connection response code {} ETag {}", vpConnection.getResponseCode(), currETag);
        }
        if ((vpConnection.getResponseCode() / 100) != 2) {
            // Nothing changed and we're out of time
            vpConnection.disconnect();
            redisClient.shutdown();
            logger.warn("No changes in VehiclePositions");
            return null;
        }

        // fetch latest values
        logger.info("Fetch positions");
        positionCollection = VehiclePositionCollection.fromInputStream(staleAge, vpConnection.getInputStream());
        logger.info("Disconnect from publisher");
        vpConnection.disconnect();
        // remove duplicates
        positionCollection.removeDuplicates(latestKeys);

        // Update persistent index
        (new VehiclePositionsIndex(currETag, positionCollection)).toRedis(redisClient);
        redisClient.shutdown();
        return positionCollection.size() != 0 ? positionCollection : null;
    }

    /**
     * Define connection for HTTP request
     * @param url URL for connection request
     * @param prevETag previous ETag to ignore identical responses
     * @return connection object
     */
    private static HttpURLConnection getHttpURLConnection(@NotNull URL url, String prevETag) {
        HttpURLConnection urlConnection = null;

        try {
            urlConnection = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert urlConnection != null;
        urlConnection.setUseCaches(true);
        if (prevETag != null) {
            urlConnection.setRequestProperty("If-None-Match", prevETag);
        }

        return urlConnection;
    }


}
