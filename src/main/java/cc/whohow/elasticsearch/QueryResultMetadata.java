package cc.whohow.elasticsearch;

public interface QueryResultMetadata {
    int getColumnCount();

    String getColumnName(int columnIndex);

    int getColumnIndex(String columnName);

    String getColumnType(int columnIndex);

    String getColumnType(String columnName);
}
