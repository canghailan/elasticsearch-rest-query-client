package cc.whohow.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    public void test() throws IOException {
        RestClient restClient = RestClient.builder(HttpHost.create(
                "")).build();
        RestQueryClient restQueryClient = new RestQueryClient(restClient);

        try (Stream<Table> stream = restQueryClient.executeQuery(Table.class, "show tables").stream()) {
            stream.forEach(System.out::println);
        }
        try (Stream<ObjectNode> stream = restQueryClient.executeQuery("select * from \".monitoring-kibana-6-2019.04.26\" limit 10").stream()) {
            stream.forEach(System.out::println);
        }

        restQueryClient.queryAsync(new QueryRequest("show tables"))
                .thenAccept(System.out::println)
                .join();
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
