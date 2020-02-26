package com.weirdocomputing.vehiclepositionsproducer;

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
import java.util.HashSet;
import java.util.Set;

/**
 * Main class for execution. This includes the CLI entry point and the
 * work function when called from other environments (e.g. Lambda)
 */
public class VehiclePositionsProducerMain {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerMain.class);

    private static final String DEFAULT_REDIS_HOST = "localhost:6379";
    private static final Duration DEFAULT_STALE_AGE = Duration.ofMinutes(60);
    private static final Instant DEFAULT_GIVE_UP_TIME = Instant.now().plus(Duration.ofSeconds(10));

    private static String redisHost = DEFAULT_REDIS_HOST;
    private static Duration staleAge = DEFAULT_STALE_AGE;
    private static Instant giveUpTime = DEFAULT_GIVE_UP_TIME;

    private static URL protobufUrl;

    static {
        String envValue = System.getenv("TRANSIT_VP_STALE_AGE_MINUTES");
        if (envValue != null && !envValue.isBlank()) {
            staleAge = Duration.ofMinutes(Integer.parseUnsignedInt(envValue));
        }
        envValue = System.getenv("TRANSIT_VP_MAX_WAIT_TIME_SECONDS");
        if (envValue != null && !envValue.isBlank()) {
            giveUpTime = Instant.now().plus(Duration.ofSeconds(Integer.parseUnsignedInt(envValue)));
        }
        try {
            protobufUrl = new URL(System.getenv("TRANSIT_VP_VEHICLE_POSITIONS_PB_URL"));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        envValue = System.getenv("TRANSIT_VP_REDIS_HOST");
        if (envValue == null || envValue.isBlank()) {
            logger.warn("Missing REDIS_HOST; using \"{}\"", redisHost);
        } else {
            redisHost = envValue;
        }

    }

    /**
     * CLI entry point
     * @param args We don't currently reference the passed arguments
     */
    public static void main(String[] args) {

        VehiclePositionCollection newPositions;

        try {
            newPositions = fetchUpdates();

            if (newPositions != null) {
                logger.info("{} new positions", newPositions.size());

                if (logger.isTraceEnabled()) {
                    for (VehiclePosition p: newPositions.values()) {
                        logger.trace("Position key: \"{}\"", p.getHashString());
                        logger.trace("Position: {}", p.toJsonObject().toString());
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
            e.printStackTrace();
            System.exit(1);
        }
        logger.info("Normal exit");
    }

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

        VehiclePositionsIndex oldVpIndex = VehiclePositionsIndex.fromRedis(redisClient);
        if (oldVpIndex == null) {
            // create an ETag that won't match anything
            prevETag = null;
            latestKeys = new HashSet<>();
            logger.info("No previous ETag");
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
