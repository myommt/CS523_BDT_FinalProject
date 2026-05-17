package com.cs523;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaConfig {
    private static final String CONFIG_FILE = "kafka.properties";
    private static Properties properties;

    static {
        properties = new Properties();
        try (FileInputStream fis = new FileInputStream(CONFIG_FILE)) {
            properties.load(fis);
        } catch (IOException e) {
            System.err.println("Warning: Could not load " + CONFIG_FILE + 
                             ", using defaults. Error: " + e.getMessage());
            properties.put("bootstrap.servers", "localhost:9092");
        }
    }

    public static String getBootstrapServers() {
        return properties.getProperty("bootstrap.servers", "localhost:9092");
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
