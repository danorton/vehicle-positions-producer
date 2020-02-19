package com.weirdocomputing.vehiclepositionsproducer;

import com.weirdocomputing.transitlib.VehiclePosition;
import com.weirdocomputing.transitlib.VehiclePositionCollection;
import org.jetbrains.annotations.NotNull;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class VehiclePositionsProducerMain {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerMain.class);

    private static final Duration STALE_AGE = Duration.ofMinutes(60);
    private static Duration staleAge;
    private static final Instant TIME_TO_GIVE_UP = Instant.now().plus(Duration.ofSeconds(10));
    private static Instant giveUpTime;
    private static String redisHost = "localhost:6379";

    private static URL protobufUrl;

    static {
        String envValue = System.getenv("TRANSIT_VP_STALE_AGE_MINUTES");
        if (envValue == null || envValue.isBlank()) {
            staleAge = STALE_AGE;
        } else {
            staleAge = Duration.ofMinutes(Integer.parseUnsignedInt(envValue));
        }
        envValue = System.getenv("TRANSIT_VP_MAX_WAIT_TIME_SECONDS");
        if (envValue == null || envValue.isBlank()) {
            giveUpTime = TIME_TO_GIVE_UP;
        } else {
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

    public static void main(String[] args) {

        HashMap<String, VehiclePosition> newPositions;

        try {
            newPositions = getUpdates();

            logger.info("{} positions read", newPositions.size());

            for (VehiclePosition p: newPositions.values()) {
                logger.trace("Position key: \"{}\"", p.getHashString());
                logger.trace("Position: {}", p.toJsonObject().toString());
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

    @NotNull
    static HashMap<String, VehiclePosition> getUpdates() throws Exception {

        VehiclePositionCollection positionCollection;
        HashMap<String, VehiclePosition> newPositions = new HashMap<>();
        String lastETag;
        Set<String> latestKeys;
//        VehicleIdAndTimestamp x; //fixme *****************



        // Read lastETag and index from Redis
        Config config = new Config();
        SingleServerConfig ssConfig = config.useSingleServer().setAddress(
                String.format("redis://%s", redisHost));
        ssConfig.setConnectionMinimumIdleSize(2);
        ssConfig.setConnectionPoolSize(2);
        config.setNettyThreads(2);
        RedissonClient redisClient = Redisson.create(config);
        VehiclePositionsIndex oldVpIndex = VehiclePositionsIndex.fromRedis(redisClient);
        if (oldVpIndex == null) {
            // create a value that won't match anything
            lastETag = String.valueOf((new Random()).nextLong());
            latestKeys = new HashSet<>();
        } else {
            lastETag = oldVpIndex.getId();
            latestKeys = oldVpIndex.getKeys();
            logger.info("Previous ETag: {}", lastETag);
        }

        // read vehicle positions
        HttpURLConnection urlConnection = getHttpURLConnection(protobufUrl);
        String newETag = urlConnection.getHeaderField("ETag");
        logger.info("Current ETag: {}", newETag);


        // If nothing changed, re-read until there's a change
        while(lastETag.equals(newETag) && Instant.now().isBefore(giveUpTime)) {
            urlConnection.disconnect();
            // sleep 1 second between retries
            boolean needSleep = true;
            do {
                try {
                    logger.info("Sleeping 1 second");
                    Thread.sleep(1000);
                    needSleep = false;
                } catch (InterruptedException e) {
                    // Ignore interrupted exception
                }
            } while (needSleep);
            urlConnection = getHttpURLConnection(protobufUrl);
            newETag = urlConnection.getHeaderField("ETag");
        }
        if (lastETag.equals(newETag)) {
            // we timed out looking for a changed response
            logger.warn("No changes in VehiclePositions");
            redisClient.shutdown();
            return newPositions;
        }
        logger.info("Current ETag: {}", newETag);
        lastETag = newETag;

        positionCollection = new VehiclePositionCollection(staleAge);

        // fetch latest values
        positionCollection.update(urlConnection.getInputStream());
        urlConnection.disconnect();
        positionCollection.removeDuplicates(latestKeys);

        // Update persistent index
        (new VehiclePositionsIndex(lastETag, positionCollection)).toRedis(redisClient);
        redisClient.shutdown();
        return newPositions;
    }

    private static HttpURLConnection getHttpURLConnection(@NotNull URL url) {
        HttpURLConnection urlConnection = null;

        try {
            urlConnection = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert urlConnection != null;
        urlConnection.setUseCaches(true);

        return urlConnection;
    }


}
