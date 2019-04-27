package cc.whohow.elasticsearch.impl;

import cc.whohow.elasticsearch.QueryCursor;
import cc.whohow.elasticsearch.QueryResult;
import cc.whohow.elasticsearch.QueryResultMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JsonObjectQueryResult implements QueryResult<ObjectNode> {
    private final JsonNodeFactory jsonNodeFactory;
    private final JsonQueryResult result;

    public JsonObjectQueryResult(JsonQueryResult result) {
        this(JsonNodeFactory.instance, result);
    }

    public JsonObjectQueryResult(ObjectMapper objectMapper, JsonQueryResult result) {
        this(objectMapper.getNodeFactory(), result);
    }

    public JsonObjectQueryResult(JsonNodeFactory jsonNodeFactory, JsonQueryResult result) {
        this.jsonNodeFactory = jsonNodeFactory;
        this.result = result;
    }

    @Override
    public QueryResultMetadata getMetadata() {
        return result;
    }

    @Override
    public QueryCursor getCursor() {
        return result;
    }

    @Override
    public boolean hasNext() {
        return result.hasNext();
    }

    @Override
    public ObjectNode next() {
        JsonNode row = result.next();
        ObjectNode object = jsonNodeFactory.objectNode();
        for (int i = 0; i < result.getColumnCount(); i++) {
            object.set(result.getColumnName(i), row.get(i));
        }
        return object;
    }

    @Override
    public void close() throws IOException {
        result.close();
    }

    public ArrayNode collect() {
        try {
            ArrayNode array = jsonNodeFactory.arrayNode();
            forEachRemaining(array::add);
            return array;
        } finally {
            closeQuietly();
        }
    }
}
