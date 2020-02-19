package com.weirdocomputing.vehiclepositionsproducer;

import com.amazonaws.Response;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.google.transit.realtime.GtfsRealtime;
import com.weirdocomputing.transitlib.VehiclePosition;
import com.weirdocomputing.transitlib.VehiclePositionCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;

// TODO - Trigger more frequently than every minute
//        (Might have continuous virtual server supporting several publishing agencies.)


public class VehiclePositionsProducerLambdaHandler implements RequestHandler<ScheduledEvent, Response<byte[]>> {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerLambdaHandler.class);

    @Override
    public Response<byte[]> handleRequest(ScheduledEvent scheduledEvent, Context context) {
        logger.info("Request from account {}", scheduledEvent.getAccount());

        Response<byte[]> response = null;

        HashMap<String, VehiclePosition> newPositions;
        try {
            newPositions = VehiclePositionsProducerMain.getUpdates();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (newPositions.size() > 0) {
            VehiclePositionCollection positions = new VehiclePositionCollection(
                    Duration.ofMinutes(60),
                    newPositions.values().toArray(new VehiclePosition[0]));
            GtfsRealtime.FeedMessage feedMessage = positions.toFeedMessage(true);
            logger.info("FeedMessage: {} entities {} bytes",
                    feedMessage.getEntityCount(),feedMessage.getSerializedSize());

            response = new Response<>(feedMessage.toByteArray(),null);
        } else {
            logger.info("Response: null");
        }

        return response;
    }
}
