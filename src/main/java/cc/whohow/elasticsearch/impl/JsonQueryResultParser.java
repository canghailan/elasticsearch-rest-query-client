package cc.whohow.elasticsearch.impl;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.base.ParserMinimalBase;
import com.fasterxml.jackson.core.json.JsonReadContext;
import com.fasterxml.jackson.core.json.PackageVersion;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonQueryResultParser extends ParserMinimalBase {
    private JsonQueryResult result;
    private JsonNode row;
    private int c = -1;
    private boolean f = true;
    private boolean eof = false;
    private boolean closed = false;

    private ObjectCodec codec;
    private JsonReadContext parsingContext = JsonReadContext.createRootContext(-1, -1, null);

    public JsonQueryResultParser(JsonQueryResult result) {
        this.result = result;
    }

    @Override
    public JsonToken nextToken() throws IOException {
        if (eof) {
            return null;
        }
        if (row == null) {
            if (parsingContext.getParent() == null) {
                // -> [
                parsingContext = parsingContext.createChildArrayContext(result.getRow(), c);
                return _currToken = JsonToken.START_ARRAY;
            } else {
                if (nextRow()) {
                    // [ -> {
                    return _currToken = JsonToken.START_OBJECT;
                } else {
                    // [ -> ]
                    return _currToken = JsonToken.END_ARRAY;
                }
            }
        }
        if (c == -1) {
            if (nextColumn()) {
                // { -> key
                return _currToken = JsonToken.FIELD_NAME;
            } else {
                // { -> }
                return _currToken = JsonToken.END_OBJECT;
            }
        }
        if (c >= result.getColumnCount()) {
            if (nextRow()) {
                // } -> {
                return _currToken = JsonToken.START_OBJECT;
            } else {
                // } -> ]
                return _currToken = JsonToken.END_ARRAY;
            }
        }
        if (f) {
            // key -> value
            f = false;
            return _currToken = row.get(c).asToken();
        }
        if (nextColumn()) {
            // value -> key
            return _currToken = JsonToken.FIELD_NAME;
        } else {
            // value -> }
            return _currToken = JsonToken.END_OBJECT;
        }
    }

    protected boolean nextRow() {
        if (result.hasNext()) {
            row = result.next();
            c = -1;
            parsingContext = parsingContext.createChildObjectContext(result.getRow(), c);
            return true;
        } else {
            eof = true;
            parsingContext = parsingContext.getParent();
            return false;
        }
    }

    protected boolean nextColumn() throws IOException {
        c++;
        if (c < result.getColumnCount()) {
            f = true;
            parsingContext.setCurrentName(result.getColumnName(c));
            parsingContext.setCurrentValue(row.get(c));
            return true;
        } else {
            parsingContext = parsingContext.getParent();
            return false;
        }
    }

    @Override
    protected void _handleEOF() throws JsonParseException {
    }

    @Override
    public String getCurrentName() throws IOException {
        return parsingContext.getCurrentName();
    }

    public JsonNode getCurrentValue() {
        return row.get(c);
    }

    @Override
    public ObjectCodec getCodec() {
        return codec;
    }

    @Override
    public void setCodec(ObjectCodec codec) {
        this.codec = codec;
    }

    @Override
    public Version version() {
        return PackageVersion.VERSION;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        result.close();
        closed = true;
    }

    public void closeQuietly() {
        try {
            close();
        } catch (IOException ignore) {
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public JsonStreamContext getParsingContext() {
        return parsingContext;
    }

    @Override
    public JsonLocation getTokenLocation() {
        return getCurrentLocation();
    }

    @Override
    public JsonLocation getCurrentLocation() {
        return new JsonLocation(result, -1, result.getRow(), c);
    }

    @Override
    public void overrideCurrentName(String name) {
        try {
            parsingContext.setCurrentName(name);
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public String getText() throws IOException {
        if (_currToken == null) {
            return null;
        }
        switch (_currToken) {
            case START_ARRAY:
            case END_ARRAY:
            case START_OBJECT:
            case END_OBJECT: {
                return _currToken.asString();
            }
            case FIELD_NAME: {
                return getCurrentName();
            }
            default: {
                return getCurrentValue().asText(null);
            }
        }
    }

    @Override
    public Number getNumberValue() throws IOException {
        return getCurrentValue().numberValue();
    }

    @Override
    public NumberType getNumberType() throws IOException {
        return getCurrentValue().numberType();
    }

    @Override
    public int getIntValue() throws IOException {
        return getCurrentValue().intValue();
    }

    @Override
    public long getLongValue() throws IOException {
        return getCurrentValue().longValue();
    }

    @Override
    public BigInteger getBigIntegerValue() throws IOException {
        return getCurrentValue().bigIntegerValue();
    }

    @Override
    public float getFloatValue() throws IOException {
        return getCurrentValue().floatValue();
    }

    @Override
    public double getDoubleValue() throws IOException {
        return getCurrentValue().doubleValue();
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        return getCurrentValue().decimalValue();
    }

    @Override
    public byte[] getBinaryValue(Base64Variant b64variant) throws IOException {
        return getCurrentValue().binaryValue();
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] getTextCharacters() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTextLength() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTextOffset() throws IOException {
        throw new UnsupportedOperationException();
    }
}
