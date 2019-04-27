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

try (Stream<Table> stream = restQueryClient.executeQuery(Table.class, "show tables").stream()) {
    stream.forEach(System.out::println);
}

System.out.println(restQueryClient.translate(new QueryRequest("select * from \".monitoring-kibana-6-2019.04.26\" limit 10")));
try (Stream<JsonNode> stream = restQueryClient.executeQuery("select * from \".monitoring-kibana-6-2019.04.26\" limit 10").stream()) {
    stream.forEach(System.out::println);
}

restQueryClient.queryAsync(new QueryRequest("show tables"))
        .thenAccept(System.out::println)
        .join();

Iterable<JsonNode> tables = restQueryClient.executeQueryAsync("show tables")
        .thenApply(QueryResult::collect)
        .join();
System.out.println(tables);
```