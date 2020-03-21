package com.weirdocomputing.vehiclepositionsproducer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.weirdocomputing.transitlib.VehiclePosition;
import com.weirdocomputing.transitlib.VehiclePositionCollection;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * An index of vehicle position reports.
 * This helps us manage updates. It is essentially an index of hashes that are
 * based vehicle ID and timestamp.
 */
public class VehiclePositionsIndex {

    /**
     * Unique ID for this index
     * If this is the same as the previous index, all the keys are the same.
     */
    private String id;

    /**
     * A set of unique keys
     */
    private Set<String> hashKeys;

    /**
     * For deserializing Json
     */
    private static final ObjectMapper mapper = new ObjectMapper();


    private static final Logger logger = LoggerFactory.getLogger(VehiclePositionsIndex.class);
    private static final JsonNodeFactory jnf = JsonNodeFactory.instance;


    /**
     * Constructor
     * @param collection VehiclePositionCollection to index
     */
    public VehiclePositionsIndex(String id, VehiclePositionCollection collection) {
        this.id = id;
        hashKeys = new HashSet<>();
        for (VehiclePosition p: collection.values()) {
            hashKeys.add(p.getHashString());
        }
    }

    /**
     * Empty constructor
     */
    private VehiclePositionsIndex() {
        this.id = "";
        hashKeys = new HashSet<>();
    }

    /**
     * Build from JsonNode
     * @param jsonNode serialized JsonNode of object to build
     * @return newly built VehiclePositionsIndex
     */
    public static VehiclePositionsIndex fromJsonObject(JsonNode jsonNode) {
        VehiclePositionsIndex newIndex = new VehiclePositionsIndex();
        newIndex.id = jsonNode.get("id").textValue();
        for (JsonNode node: jsonNode.withArray("keys")) {
            newIndex.hashKeys.add(node.textValue());
        }
        return newIndex;
    }

    /**
     * Build instance from value in Redis
     * @param redisClient Access to redis
     * @return new instance
     * @throws Exception If JSON string is invalid
     */
    public static VehiclePositionsIndex fromRedis(RedisClient redisClient) throws Exception {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        VehiclePositionsIndex result = null;
        logger.debug("+fromRedis: GET");
        String vpIndexJsonString = syncCommands.get("VehiclePositionsIndex");
        if (vpIndexJsonString != null) {
            logger.debug("-fromRedis: Size: {}", vpIndexJsonString.length());
            result = VehiclePositionsIndex.fromJson(vpIndexJsonString);
        } else {
            logger.debug("-fromRedis: (nil)");
        }
        return result;
    }

    /**
     * Build instance from Jackson JsonNode
     * @param jsonNode Node containing serialized instance
     * @return new instance
     */
    public static VehiclePositionsIndex fromJson(JsonNode jsonNode) {
        VehiclePositionsIndex newIndex = new VehiclePositionsIndex();
        newIndex.id = jsonNode.get("id").textValue();
        for (final JsonNode keyNode: jsonNode.get("keys")) {
            newIndex.hashKeys.add(keyNode.textValue());
        }
        return newIndex;
    }

    /**
     * Build instance from JSON string
     * @param jsonString Serialized instance as a JSON string
     * @return new instance
     * @throws Exception If JSON string is invalid
     */
    public static VehiclePositionsIndex fromJson(String jsonString) throws Exception {
        return jsonString != null
                ? fromJson(mapper.readTree(jsonString))
                : null;
    }

    /**
     * Serialize this object as a JSON object
     * @return JsonNode of this object
     */
    public JsonNode toJsonObject() {
        ObjectNode result = jnf.objectNode();
        result.put("id", this.id);
        ArrayNode ar = result.putArray ("keys");
        for (String key: this.hashKeys) {
            ar.add(key);
        }
        return result;
    }

    /**
     * Serialize to Redis server
     * @param redisClient Lettuce client object
     */
    public void toRedis(RedisClient redisClient) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        syncCommands.set("VehiclePositionsIndex",
            this.toJsonObject().toString(),
            SetArgs.Builder.ex(300));
    }

    public String getId() {
        return id;
    }

    public Set<String> getKeys() {
        return hashKeys;
    }
}
