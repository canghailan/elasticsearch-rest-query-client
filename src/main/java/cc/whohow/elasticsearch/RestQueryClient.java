package cc.whohow.elasticsearch;

import cc.whohow.elasticsearch.impl.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class RestQueryClient {
    private final String query = "/_xpack/sql?format=json";
    private final String close = "/_xpack/sql/close";
    private final RestClient restClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RestQueryClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public QueryResult<ObjectNode> executeQuery(String query) throws IOException {
        return executeQuery(new QueryRequest(query));
    }

    public QueryResult<ObjectNode> executeQuery(QueryRequest query) throws IOException {
        return mapping(new JsonQueryResultFetcher(this, query));
    }

    public <T> QueryResult<T> executeQuery(Class<T> type, String query) throws IOException {
        return executeQuery(type, new QueryRequest(query));
    }

    public <T> QueryResult<T> executeQuery(Class<T> type, QueryRequest query) throws IOException {
        return mapping(new JsonQueryResultFetcher(this, query), type);
    }

    public CompletableFuture<QueryResult<ObjectNode>> executeQueryAsync(String query) {
        return executeQueryAsync(new QueryRequest(query));
    }

    public CompletableFuture<QueryResult<ObjectNode>> executeQueryAsync(QueryRequest query) {
        return queryAsync(query)
                .thenApply(this::parse)
                .thenApply(this::mapping);
    }

    public <T> CompletableFuture<QueryResult<T>> executeQueryAsync(Class<T> type, String query) {
        return executeQueryAsync(type, new QueryRequest(query));
    }

    public <T> CompletableFuture<QueryResult<T>> executeQueryAsync(Class<T> type, QueryRequest query) {
        return queryAsync(query)
                .thenApply(this::parse)
                .thenApply(result -> mapping(result, type));
    }

    public void close(String cursor) throws IOException {
        restClient.performRequest(
                HttpPost.METHOD_NAME,
                close,
                Collections.emptyMap(),
                jsonBody(Collections.singletonMap("cursor", cursor)));
    }

    public JsonNode query(QueryRequest queryRequest) throws IOException {
        Response response = restClient.performRequest(
                HttpPost.METHOD_NAME,
                query,
                Collections.emptyMap(),
                jsonBody(queryRequest));
        return json(response);
    }

    public CompletableFuture<JsonNode> queryAsync(QueryRequest queryRequest) {
        try {
            CompletableResponseListener listener = new CompletableResponseListener();
            restClient.performRequestAsync(
                    HttpPost.METHOD_NAME,
                    query,
                    Collections.emptyMap(),
                    jsonBody(queryRequest),
                    listener);
            return listener.get().thenApply(this::json);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    protected HttpEntity jsonBody(Object object) throws IOException {
        ByteBufferOutputStream buffer = new ByteBufferOutputStream(1024);
        objectMapper.writeValue(buffer, object);
        ByteBuffer byteBuffer = buffer.getByteBuffer();
        byteBuffer.flip();
        return new JsonHttpEntity(byteBuffer);
    }

    protected JsonNode json(Response response) {
        try (InputStream stream = response.getEntity().getContent()) {
            return objectMapper.readTree(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public JsonQueryResult parse(JsonNode result) {
        return new JsonQueryResult(result);
    }

    public JsonObjectQueryResult mapping(JsonQueryResult result) {
        return new JsonObjectQueryResult(objectMapper, result);
    }

    public <T> ObjectQueryResult<T> mapping(JsonQueryResult result, Class<T> type) {
        try {
            return new ObjectQueryResult<>(objectMapper, type, result);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
