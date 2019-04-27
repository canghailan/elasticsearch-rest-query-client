package cc.whohow.elasticsearch.impl;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class JsonHttpEntity implements HttpEntity {
    private static final Header CONTENT_TYPE = new BasicHeader(HTTP.CONTENT_TYPE, "application/json;charset=utf-8");

    private final ByteBuffer buffer;

    public JsonHttpEntity(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public boolean isChunked() {
        return false;
    }

    @Override
    public long getContentLength() {
        return buffer.remaining();
    }

    @Override
    public Header getContentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Header getContentEncoding() {
        return null;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return new ByteBufferInputStream(buffer.duplicate());
    }

    @Override
    public void writeTo(OutputStream stream) throws IOException {
        if (buffer.hasArray()) {
            stream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            ByteBuffer copy = buffer.duplicate();
            while (copy.hasRemaining()) {
                stream.write(copy.get());
            }
        }
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @Override
    public void consumeContent() throws IOException {
    }
}
