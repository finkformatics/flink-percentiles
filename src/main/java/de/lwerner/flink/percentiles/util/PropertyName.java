package de.lwerner.flink.percentiles.util;

/**
 * Enum PropertyName
 *
 * Holds the clearly defined property keys for the properties file. So we cope with spelling errors and such things.
 *
 * @author Lukas Werner
 */
public enum PropertyName {

    /**
     * Redis host
     */
    REDIS_HOST("redis.host"),
    /**
     * Redis port
     */
    REDIS_PORT("redis.port"),
    /**
     * Redis adapter to use
     */
    REDIS_ADAPTER("redis.adapter");

    /**
     * The actual property key
     */
    final String propertyKey;

    /**
     * Constructor to hold the property key
     *
     * @param propertyKey actual property key
     */
    PropertyName(final String propertyKey) {
        this.propertyKey = propertyKey;
    }

}