package de.lwerner.flink.percentiles.util;

import redis.clients.jedis.Jedis;

/**
 * Helper class for comfortably working with Jedis, a Redis client library
 *
 * @author Lukas Werner
 */
public class JedisHelper {

    /**
     * Redis key for the k value
     */
    private static final String REDIS_KEY_K = "flink-percentiles-k";
    /**
     * Redis key for the n value
     */
    private static final String REDIS_KEY_N = "flink-percentiles-n";
    /**
     * Redis key for the t value
     */
    private static final String REDIS_KEY_T = "flink-percentiles-t";
    /**
     * Redis key for the resultFound value
     */
    private static final String REDIS_KEY_RESULT_FOUND = "flink-percentiles-result-found";
    /**
     * Redis key for the result value
     */
    private static final String REDIS_KEY_RESULT = "flink-percentiles-result";
    /**
     * Redis key for the number of k
     */
    private static final String REDIS_KEY_NUMBER_OF_K = "flink-percentiles-number-of-k";

    /**
     * Build a Jedis object
     *
     * @param host the redis host
     * @param port the redis port
     *
     * @return the built Jedis object
     */
    public static Jedis getJedis(String host, int port) {
        return new Jedis(host, port);
    }

    /**
     * Set a new value for nth k
     *
     * @param jedis the Jedis object
     * @param k the new k value
     * @param n the nth k
     */
    public static void setK(Jedis jedis, long k, int n) {
        jedis.set(REDIS_KEY_K + "-" + n, "" + k);
    }

    /**
     * Set a new value for n
     *
     * @param jedis the Jedis object
     * @param n the new n value
     */
    public static void setN(Jedis jedis, long n) {
        jedis.set(REDIS_KEY_N, "" + n);
    }

    /**
     * Set a new value for t
     *
     * @param jedis the Jedis object
     * @param t the new t value
     */
    public static void setT(Jedis jedis, long t) {
        jedis.set(REDIS_KEY_T, "" + t);
    }

    /**
     * Set a new value for resultFound
     *
     * @param jedis the Jedis object
     * @param resultFound the new resultFound value
     */
    public static void setResultFound(Jedis jedis, boolean resultFound) {
        jedis.set(REDIS_KEY_RESULT_FOUND, "" + resultFound);
    }

    /**
     * Set a new value for result
     *
     * @param jedis the Jedis object
     * @param result the new result value
     */
    public static void setResult(Jedis jedis, float result) {
        jedis.set(REDIS_KEY_RESULT, "" + result);
    }

    /**
     * Set number of k values
     *
     * @param jedis the Jedis object
     * @param numberOfKValues new number of k
     */
    public static void setNumberOfKValues(Jedis jedis, int numberOfKValues) {
        jedis.set(REDIS_KEY_NUMBER_OF_K, "" + numberOfKValues);
    }

    /**
     * Get the current value for k
     *
     * @param jedis the Jedis object
     * @param n nth k
     *
     * @return the current value for k
     */
    public static long getK(Jedis jedis, int n) {
        return Long.valueOf(jedis.get(REDIS_KEY_K + "-" + n));
    }

    /**
     * Get the current value for n
     *
     * @param jedis the Jedis object
     *
     * @return the current value for n
     */
    public static long getN(Jedis jedis) {
        return Long.valueOf(jedis.get(REDIS_KEY_N));
    }

    /**
     * Get the current value for t
     *
     * @param jedis the Jedis object
     *
     * @return the current value for t
     */
    public static long getT(Jedis jedis) {
        return Long.valueOf(jedis.get(REDIS_KEY_T));
    }

    /**
     * Get the current value for resultFound
     *
     * @param jedis the Jedis object
     *
     * @return the current value for resultFound
     */
    public static boolean getResultFound(Jedis jedis) {
        return Boolean.valueOf(jedis.get(REDIS_KEY_RESULT_FOUND));
    }

    /**
     * Get the current value for result
     *
     * @param jedis the Jedis object
     *
     * @return the current value for result
     */
    public static float getResult(Jedis jedis) {
        return Float.valueOf(jedis.get(REDIS_KEY_RESULT));
    }

    /**
     * Get the number of k
     *
     * @param jedis the Jedis object
     *
     * @return the number of k
     */
    public static int getNumberOfKValues(Jedis jedis) {
        return Integer.valueOf(jedis.get(REDIS_KEY_NUMBER_OF_K));
    }

    /**
     * Increase the number of k by 1
     *
     * @param jedis the Jedis object
     */
    public static void incrementNumberOfKValues(Jedis jedis) {
        setNumberOfKValues(jedis, getNumberOfKValues(jedis) + 1);
    }

}