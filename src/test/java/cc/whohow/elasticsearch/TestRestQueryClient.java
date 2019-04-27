package cc.whohow.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Stream;

public class TestRestQueryClient {
    @Test
    public void testQueryRequest() throws IOException {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setQuery("show tables");
        queryRequest.setFetchSize(1000);
        System.out.println(new ObjectMapper().writeValueAsString(queryRequest));
    }

    @Test
    public void testPreparedQueryRequest() throws IOException {
        PreparedQueryRequest preparedQueryRequest = new PreparedQueryRequest(
                "select * from \":table\" limit :cnt"
        );
        QueryRequest queryRequest = preparedQueryRequest.apply(
                "table", ".monitoring-kibana-6-2019.04.26",
                "cnt", 10);
        System.out.println(queryRequest.getQuery());
    }

    @Test
    public void test() throws IOException {
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
    }

    static class Table {
        private String name;
        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
