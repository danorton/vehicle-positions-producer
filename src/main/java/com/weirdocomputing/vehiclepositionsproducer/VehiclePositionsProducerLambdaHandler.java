package com.weirdocomputing.vehiclepositionsproducer;


import com.amazonaws.Response;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.weirdocomputing.transitlib.VehiclePosition;
import com.weirdocomputing.transitlib.VehiclePositionCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

// TODO - Trigger more frequently than every minute
//        (Might have continuous virtual server supporting several publishing agencies.)


public class VehiclePositionsProducerLambdaHandler implements RequestHandler<ScheduledEvent, Response<String>> {
    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsProducerLambdaHandler.class);

    @Override
    public Response<String> handleRequest(ScheduledEvent scheduledEvent, Context context) {
        logger.info("Request from account {}", scheduledEvent.getAccount());

        Response<String> response = null;

        HashMap<String, VehiclePosition> newPositions;
        try {
            newPositions = VehiclePositionsProducerMain.getUpdates();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (newPositions.size() > 0) {
            ArrayNode rawResponse = VehiclePositionCollection.toJsonArray(newPositions.values());
            String responseString = rawResponse.toString();
            response = new Response<>(responseString,null);
            logger.info(String.format("Response: %d positions %d characters",
                    newPositions.size(),
                    responseString.length()));
        } else {
            logger.info("Response: null");
        }

        return response;
    }
}
