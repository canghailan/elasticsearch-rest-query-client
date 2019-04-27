package cc.whohow.elasticsearch.impl;

import cc.whohow.elasticsearch.QueryCursor;
import cc.whohow.elasticsearch.QueryResultMetadata;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JsonQueryResult implements QueryResultMetadata, QueryCursor, Iterator<JsonNode>, Closeable {
    protected List<String> columnNames;
    protected List<String> columnTypes;
    protected Iterator<JsonNode> rows;
    protected String cursor;
    protected int row = -1;

    public JsonQueryResult() {
    }

    public JsonQueryResult(JsonNode result) {
        parse(result);
    }

    protected int parse(JsonNode result) {
        JsonNode columns = result.get("columns");
        if (columns != null) {
            columnNames = new ArrayList<>(columns.size());
            columnTypes = new ArrayList<>(columns.size());
            for (JsonNode column : columns) {
                columnNames.add(column.path("name").textValue());
                columnTypes.add(column.path("type").textValue());
            }
        }
        JsonNode rows = result.path("rows");
        this.rows = rows.iterator();
        cursor = result.path("cursor").textValue();
        return rows.size();
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public String getColumnName(int columnIndex) {
        return columnNames.get(columnIndex);
    }

    @Override
    public int getColumnIndex(String columnName) {
        return columnNames.indexOf(columnName);
    }

    @Override
    public String getColumnType(int columnIndex) {
        return columnTypes.get(columnIndex);
    }

    @Override
    public String getColumnType(String columnName) {
        return getColumnType(getColumnIndex(columnName));
    }

    @Override
    public String getCursor() {
        return cursor;
    }

    public int getRow() {
        return row;
    }

    @Override
    public boolean hasNext() {
        return rows.hasNext();
    }

    @Override
    public JsonNode next() {
        row++;
        return rows.next();
    }

    @Override
    public void close() throws IOException {

    }
}
