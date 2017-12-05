package de.lwerner.flink.percentiles.model;

import java.io.Serializable;

/**
 * Holder for redis connection information
 *
 * @author Lukas Werner
 */
public class RedisCredentials implements Serializable {

    /**
     * The adapter (jedis, fake)
     */
    private String adapter;
    /**
     * The redis host
     */
    private String host;
    /**
     * The redis port
     */
    private int port;
    /**
     * Redis password for auth
     */
    private String password;

    /**
     * Get the adapter
     *
     * @return the adapter
     */
    public String getAdapter() {
        return adapter;
    }

    /**
     * Set an adapter
     *
     * @param adapter the adapter to set
     */
    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    /**
     * Get the redis host
     *
     * @return the redis host
     */
    public String getHost() {
        return host;
    }

    /**
     * Set the redis host
     *
     * @param host the redis host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Get the redis port
     *
     * @return the redis port
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the redis port
     *
     * @param port the redis port to set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Get the redis password
     *
     * @return the redis password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set the redis password
     *
     * @param password the redis password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }
}