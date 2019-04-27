# Elasticsearch SQL 查询工具

* 仅依赖RestClient、Jackson
* 支持Stream
* 支持CompletableFuture
* 同步模式自动处理Cursor
* Lazy读取、解析
* 初步支持查询参数


示例代码：
```java
RestClient restClient = RestClient.builder(HttpHost.create(
        "")).build();
RestQueryClient restQueryClient = new RestQueryClient(restClient);

String showTables = "show tables";

// 原始异步查询
restQueryClient.queryAsync(new QueryRequest(showTables))
        .thenAccept(System.out::println)
        .join();

// 异步查询
Iterable<JsonNode> tables = restQueryClient.executeQueryAsync(showTables)
        .thenApply(QueryResult::collect)
        .join();
System.out.println(tables);

// 流式查询，并转为实体对象
try (Stream<Table> stream = restQueryClient.executeQuery(Table.class, showTables).stream()) {
    stream.forEach(System.out::println);
}

// 模板查询
PreparedQueryRequest preparedSelect = new PreparedQueryRequest(
        "select * from \":table\" limit :cnt"
);
// 查询语句生成
QueryRequest select = preparedSelect.apply(
        "table", ".monitoring-kibana-6-2019.04.26",
        "cnt", 20);
select.setFetchSize(10);

// 翻译查询
System.out.println(restQueryClient.translate(select));
// 流式查询，支持Cursor自动处理
try (Stream<JsonNode> stream = restQueryClient.executeQuery(select).stream()) {
    stream.forEach(System.out::println);
}
```