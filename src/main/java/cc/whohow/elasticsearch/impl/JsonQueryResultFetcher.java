package cc.whohow.elasticsearch.impl;

import cc.whohow.elasticsearch.QueryRequest;
import cc.whohow.elasticsearch.RestQueryClient;

import java.io.IOException;
import java.io.UncheckedIOException;

public class JsonQueryResultFetcher extends JsonQueryResult {
    protected final RestQueryClient client;
    protected final QueryRequest query;

    public JsonQueryResultFetcher(RestQueryClient client, QueryRequest query) {
        this.client = client;
        this.query = query;
    }

    protected int fetch() throws IOException {
        if (rows == null) {
            return parse(client.query(query));
        }
        if (cursor != null) {
            QueryRequest query = new QueryRequest();
            query.setCursor(cursor);
            return parse(client.query(query));
        }
        return 0;
    }

    protected void ensure(Object object) {
        if (object == null) {
            try {
                fetch();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public int getColumnCount() {
        ensure(columnNames);
        return super.getColumnCount();
    }

    public String getColumnName(int columnIndex) {
        ensure(columnNames);
        return super.getColumnName(columnIndex);
    }

    public int getColumnIndex(String columnName) {
        ensure(columnNames);
        return super.getColumnIndex(columnName);
    }

    public String getColumnType(int columnIndex) {
        ensure(columnTypes);
        return super.getColumnType(columnIndex);
    }

    public String getCursor() {
        ensure(cursor);
        return cursor;
    }

    @Override
    public boolean hasNext() {
        ensure(rows);
        if (rows.hasNext()) {
            return true;
        }
        try {
            if (fetch() == 0) {
                return false;
            }
            return rows.hasNext();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (cursor != null) {
            client.close(cursor);
            cursor = null;
        }
    }
}
