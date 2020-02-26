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

/**
 * This is the entry point when operating as an AWS Lambda function.
 * It turns at that this is not practical because we require Internet
 * access. That requires a NAT gateway, currently charged at the rate
 * of $0.045/hr ($1.08/day).
 *
 * Also, as a Lamda function, we'd like to trigger as often as the data
 * is updated, requiring a sub-minute trigger. This can be done with the
 * addition of another Lambda and a state machine.
 */
public class VehiclePositionsProducerLambdaHandler implements RequestHandler<ScheduledEvent, Response<byte[]>> {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerLambdaHandler.class);

    /**
     * Entry point for AWS Lambda Function
     * @param scheduledEvent input that triggered the function
     * @param context Environment
     * @return SQS message containing binary protobuf of changed positions
     */
    @Override
    public Response<byte[]> handleRequest(ScheduledEvent scheduledEvent, Context context) {
        logger.info("Request from account {}", scheduledEvent.getAccount());

        Response<byte[]> response = null;

        VehiclePositionCollection newPositions;
        try {
            newPositions = VehiclePositionsProducerMain.fetchUpdates();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (newPositions != null && newPositions.size() > 0) {
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
