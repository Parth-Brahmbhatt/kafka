package org.apache.kafka.rest.sink;


import java.io.Closeable;
import java.io.IOException;

public interface KeyValueSink extends Closeable {

    public void store(String key, byte[] data) throws IOException;
    public void store(String key, byte[] data, long timestamp) throws IOException;
    public void delete(String key) throws IOException;

}