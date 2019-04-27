package cc.whohow.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryRequest {
    private String query;
    @JsonProperty("fetch_size")
    private Integer fetchSize;
    @JsonIgnore
    private Integer gte;
    @JsonIgnore
    private Integer lte;
    @JsonProperty("request_timeout")
    private Integer requestTimeout;
    @JsonProperty("page_timeout")
    private Integer pageTimeout;
    @JsonProperty("time_zone")
    private String timeZone;
    @JsonProperty("field_multi_value_leniency")
    private Boolean fieldMultiValueLeniency;
    private String cursor;

    public QueryRequest() {
    }

    public QueryRequest(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public Integer getGte() {
        return gte;
    }

    public void setGte(Integer gte) {
        this.gte = gte;
    }

    public Integer getLte() {
        return lte;
    }

    public void setLte(Integer lte) {
        this.lte = lte;
    }

    public Integer getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Integer requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public Integer getPageTimeout() {
        return pageTimeout;
    }

    public void setPageTimeout(Integer pageTimeout) {
        this.pageTimeout = pageTimeout;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public Boolean getFieldMultiValueLeniency() {
        return fieldMultiValueLeniency;
    }

    public void setFieldMultiValueLeniency(Boolean fieldMultiValueLeniency) {
        this.fieldMultiValueLeniency = fieldMultiValueLeniency;
    }

    public String getCursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public void withOffsetFetch(int begin, int rows) {
        this.gte = begin;
        this.lte = begin + rows;
    }

    public Object getFilter() {
        if (gte == null && lte == null) {
            return null;
        }
        Map<String, Integer> object = new LinkedHashMap<>(2, 1);
        if (gte != null) {
            object.put("gte", gte);
        }
        if (lte != null) {
            object.put("lte", lte);
        }
        return Collections.singletonMap("range",
                Collections.singletonMap("page_count", object));
    }
}
