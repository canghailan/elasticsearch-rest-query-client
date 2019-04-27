package cc.whohow.elasticsearch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface QueryResult<T> extends Iterator<T>, Closeable {
    QueryResultMetadata getMetadata();

    QueryCursor getCursor();

    default void closeQuietly() {
        try {
            close();
        } catch (IOException ignore) {
        }
    }

    default Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, 0), false)
                .onClose(this::closeQuietly);
    }

    Iterable<T> collect();
}
