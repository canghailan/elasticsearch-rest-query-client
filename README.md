# Elasticsearch SQL 查询工具

* 仅依赖RestClient、Jackson
* 支持Stream
* 支持CompletableFuture
* 同步模式自动处理Cursor
* Lazy读取、解析


示例代码：
```java
RestClient restClient = RestClient.builder(HttpHost.create(
        "")).build();
RestQueryClient restQueryClient = new RestQueryClient(restClient);

try (Stream<Table> stream = restQueryClient.executeQuery(Table.class, "show tables").stream()) {
    stream.forEach(System.out::println);
}
try (Stream<JsonNode> stream = restQueryClient.executeQuery("select * from \".monitoring-kibana-6-2019.04.26\" limit 10").stream()) {
    stream.forEach(System.out::println);
}

restQueryClient.queryAsync(new QueryRequest("show tables"))
        .thenAccept(System.out::println)
        .join();

Iterable<JsonNode> array = restQueryClient.executeQueryAsync("show tables")
        .thenApply(QueryResult::collect)
        .join();
System.out.println(array);
```