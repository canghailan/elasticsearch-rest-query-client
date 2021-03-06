package cc.whohow.elasticsearch.impl;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferOutputStream extends OutputStream implements WritableByteChannel {
    protected ByteBuffer byteBuffer;

    public ByteBufferOutputStream() {
        this(32);
    }

    public ByteBufferOutputStream(int size) {
        this.byteBuffer = ByteBuffer.allocate(size);
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    private void ensureByteBufferRemaining(int minRemaining) {
        if (byteBuffer.remaining() < minRemaining) {
            int growth = byteBuffer.capacity();
            if (growth < minRemaining) {
                growth = minRemaining;
            }
            byteBuffer = copyOf(byteBuffer, byteBuffer.capacity() + growth);
        }
    }

    private ByteBuffer copyOf(ByteBuffer byteBuffer, int capacity) {
        byteBuffer.flip();
        return ByteBuffer.allocate(capacity).put(byteBuffer);
    }

    public int write(ByteBuffer b) {
        int n = b.remaining();
        ensureByteBufferRemaining(n);
        byteBuffer.put(b);
        return n;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ensureByteBufferRemaining(len);
        byteBuffer.put(b, off, len);
    }

    @Override
    public void write(int b) {
        ensureByteBufferRemaining(1);
        byteBuffer.put((byte) b);
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
