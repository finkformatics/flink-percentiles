package de.lwerner.flink.percentiles.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class AppProperties
 *
 * Improves the Java default Properties class by having clearly defined property keys and NO write access.
 *
 * @author Lukas Werner
 */
public class AppProperties {

    /**
     * The file path to properties file
     */
    private static final String FILE_PATH = "/application.properties";

    /**
     * The instance (Singleton)
     */
    private static AppProperties instance;

    /**
     * The actual properties, wrapped by this class
     */
    private Properties properties = new Properties();

    /**
     * Private constructor prevents from breaking the singleton pattern. Only succeeds, if properties were loaded
     * properly
     *
     * @throws IOException if the properties file couldn't be loaded
     */
    private AppProperties() throws IOException {
        readAppProperties();
    }

    /**
     * Returns the property by given key. If property isn't present, it'll return null
     *
     * @param key the property key (from enum)
     *
     * @return the value or null, if key not present
     */
    public synchronized String getProperty(final PropertyName key) {
        return getProperty(key, null);
    }

    /**
     * Returns the property by given key. If property isn't present, it'll return the given defaultValue
     *
     * @param key the property key (from enum)
     * @param defaultValue defined default value
     *
     * @return the value or defaultValue, if key not present
     */
    public synchronized String getProperty(final PropertyName key, String defaultValue) {
        return properties.getProperty(key.propertyKey, defaultValue);
    }

    /**
     * Actually reads the properties file by using java.util.Properties::load(InputStream in) method
     *
     * @throws IOException if the properties couldn't be loaded
     */
    private synchronized void readAppProperties() throws IOException {
        try(InputStream in = AppProperties.class.getResourceAsStream(FILE_PATH)) {
            properties.load(in);
        }
    }

    /**
     * The singleton method. Instantiates the instance if not yet happened and returns it.
     *
     * @return the AppProperties singleton instance
     *
     * @throws IOException if the properties file couldn't be loaded
     */
    public static synchronized AppProperties getInstance() throws IOException {
        if (instance == null) {
            instance = new AppProperties();
        }

        return instance;
    }

}