package com.weirdocomputing.vehiclepositionsproducer;

import com.weirdocomputing.transitlib.VehiclePosition;
import com.weirdocomputing.transitlib.VehiclePositionCollection;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;


public class VehiclePositionsProducerMain {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerMain.class);

    private static final Duration STALE_AGE = Duration.ofMinutes(60);
    private static Duration staleAge;
    private static final Instant TIME_TO_GIVE_UP = Instant.now().plus(Duration.ofSeconds(10));
    private static Instant giveUpTime;

    private static URL protobufUrl;

    static {
        String envValue = System.getenv("STALE_AGE_MINUTES");
        if (envValue == null || envValue.isBlank()) {
            staleAge = STALE_AGE;
        } else {
            staleAge = Duration.ofMinutes(Integer.parseUnsignedInt(envValue));
        }
        envValue = System.getenv("MAX_WAIT_TIME_SECONDS");
        if (envValue == null || envValue.isBlank()) {
            giveUpTime = TIME_TO_GIVE_UP;
        } else {
            giveUpTime = Instant.now().plus(Duration.ofSeconds(Integer.parseUnsignedInt(envValue)));
        }
        try {
            protobufUrl = new URL(System.getenv("VEHICLE_POSITIONS_PB_URL"));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        HashMap<String, VehiclePosition> newPositions;

        try {
            newPositions = getUpdates();

            logger.info("{} positions read", newPositions.size());

            for (VehiclePosition p: newPositions.values()) {
                logger.info("Position: {}", p.toJsonObject().toString());
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

        HttpURLConnection urlConnection = getHttpURLConnection(protobufUrl);
        VehiclePositionCollection positionCollection;
        HashMap<String, VehiclePosition> oldPositions, newPositions = new HashMap<>();
        String newEtag = urlConnection.getHeaderField("ETag");
        String lastETag;

        // TODO read lastETag and previous positions from redis
        lastETag = "dummy";
        oldPositions = new HashMap<>();

        // If nothing changed, re-read until there's a change
        while(lastETag.equals(newEtag) && Instant.now().isBefore(giveUpTime)) {
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
            newEtag = urlConnection.getHeaderField("ETag");
        }
        if (lastETag.equals(newEtag)) {
            logger.warn("No changes in VehiclePositions");
            return newPositions;
        }

        positionCollection = new VehiclePositionCollection(staleAge);
        positionCollection.addPositions(oldPositions);

        // fetch values
        newPositions = positionCollection.update(urlConnection.getInputStream());
        urlConnection.disconnect();

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
