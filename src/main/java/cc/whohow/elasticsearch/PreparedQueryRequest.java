package cc.whohow.elasticsearch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreparedQueryRequest extends QueryRequest
        implements Function<Function<String, ?>, QueryRequest> {
    private static final Pattern PATTERN = Pattern.compile(":(?<name>[a-zA-Z0-9_]+)");

    public PreparedQueryRequest() {
    }

    public PreparedQueryRequest(String query) {
        super(query);
    }

    public QueryRequest apply(Object... params) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < params.length; i += 2) {
            map.put(params[i].toString(), params[i + 1]);
        }
        return apply(map);
    }

    public QueryRequest apply(Map<String, ?> params) {
        return apply(params::get);
    }

    @Override
    public QueryRequest apply(Function<String, ?> params) {
        StringBuffer buffer = new StringBuffer();
        Matcher matcher = PATTERN.matcher(getQuery());
        while (matcher.find()) {
            String name = matcher.group("name");
            String value = toString(params.apply(name));
            matcher.appendReplacement(buffer, value);
        }
        matcher.appendTail(buffer);

        QueryRequest queryRequest = new QueryRequest(buffer.toString());
        queryRequest.setFetchSize(getFetchSize());
        queryRequest.setGte(getGte());
        queryRequest.setLte(getLte());
        queryRequest.setRequestTimeout(getRequestTimeout());
        queryRequest.setPageTimeout(getPageTimeout());
        queryRequest.setTimeZone(getTimeZone());
        queryRequest.setFieldMultiValueLeniency(getFieldMultiValueLeniency());
        return queryRequest;
    }

    protected String toString(Object param) {
        if (param == null) {
            return "null";
        }
        return param.toString();
    }
}
