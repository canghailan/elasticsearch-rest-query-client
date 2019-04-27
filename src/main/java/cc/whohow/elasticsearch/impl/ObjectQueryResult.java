package cc.whohow.elasticsearch.impl;

import cc.whohow.elasticsearch.QueryCursor;
import cc.whohow.elasticsearch.QueryResult;
import cc.whohow.elasticsearch.QueryResultMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ObjectQueryResult<T> implements QueryResult<T> {
    private final JsonQueryResult result;
    private final Iterator<T> iterator;

    public ObjectQueryResult(ObjectMapper objectMapper, Class<T> type, JsonQueryResult result) throws IOException {
        JsonQueryResultParser parser = new JsonQueryResultParser(result);
        // skip START_ARRAY
        parser.nextToken();
        parser.clearCurrentToken();
        // read values
        this.result = result;
        this.iterator = objectMapper.readValues(parser, type);
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
    public void close() throws IOException {
        result.close();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public List<T> collect() {
        try {
            List<T> list = new ArrayList<>();
            forEachRemaining(list::add);
            return list;
        } finally {
            closeQuietly();
        }
    }
}
