package org.apache.kafka.rest.sink;

import java.util.Properties;

public class SinkConfiguration {

    private Properties props;

    public SinkConfiguration() {
        this(new Properties());
    }

    public SinkConfiguration(Properties props) {
        this.props = props;
    }

    public String getString(String key) {
        return props.getProperty(key);
    }

    public String getString(String key, String def) {
        return props.containsKey(key) ? props.getProperty(key) : def;
    }

    public void setString(String key, String value) {
        if (key == null || value == null) {
            return;
        }
        props.setProperty(key, value);
    }

    public int getInt(String key) {
        return Integer.parseInt(props.getProperty(key));
    }

    public int getInt(String key, int def) {
        return props.containsKey(key) ? Integer.parseInt(props.getProperty(key)) : def;
    }

    public void setInt(String key, int value) {
        props.setProperty(key, String.valueOf(value));
    }

    public long getLong(String key) {
        return Long.parseLong(props.getProperty(key));
    }

    public long getLong(String key, long def) {
        return props.containsKey(key) ? Long.parseLong(props.getProperty(key)) : def;
    }

    public void setLong(String key, long value) {
        props.setProperty(key, String.valueOf(value));
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(props.getProperty(key));
    }

    public boolean getBoolean(String key, boolean def) {
        return props.containsKey(key) ? Boolean.parseBoolean(props.getProperty(key)) : def;
    }

    public void setBoolean(String key, boolean value) {
        props.setProperty(key, String.valueOf(value));
    }
}
