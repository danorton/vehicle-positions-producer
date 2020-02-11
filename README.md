# GTFS vehicle positions producer

Read GTFS-realtime formatted VehiclePositions and relay them to an Amazon AWS SQS queue.
Keep track of last state and remove duplicates.
