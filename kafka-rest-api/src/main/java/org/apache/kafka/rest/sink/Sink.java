package org.apache.kafka.rest.sink;

import java.io.IOException;

public interface Sink {

    public void store(byte[] data) throws IOException;

}
