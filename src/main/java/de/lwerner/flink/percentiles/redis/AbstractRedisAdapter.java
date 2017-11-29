package de.lwerner.flink.percentiles.redis;

import de.lwerner.flink.percentiles.util.AppProperties;
import de.lwerner.flink.percentiles.util.PropertyName;

import java.io.Serializable;

/**
 * Class AbstractRedisAdapter
 *
 * Holds the abstract methods for working with redis in this application and provides a factory method.
 *
 * @author Lukas Werner
 */
public abstract class AbstractRedisAdapter implements Serializable {

    /**
     * The Jedis adapter name
     */
    private static final String REDIS_ADAPTER_JEDIS = "jedis";
    /**
     * The adapter name for a redis simulation
     */
    private static final String REDIS_ADAPTER_FAKE = "fake";

    /**
     * Redis password, not necessarily required
     */
    private static String redisPassword = null;

    /**
     * Gets the N value (holds the current amount of elements, remaining in algorithm)
     *
     * @return N
     */
    public abstract long getN();

    /**
     * Sets the N value
     *
     * @param n new N
     */
    public abstract void setN(long n);

    /**
     * Adds a new k value
     *
     * @param k new k value
     */
    public abstract void addK(long k);

    /**
     * Get the whole k array
     *
     * @return all k
     */
    public abstract long[] getAllK();

    /**
     * Get the nth k
     *
     * @param n starting at 1
     *
     * @return nth k
     */
    public abstract long getNthK(int n);

    /**
     * Set the nth k
     *
     * @param k k
     * @param n nth
     */
    public abstract void setNthK(long k, int n);

    /**
     * Gets the T value (threshold of ending the algorithm)
     *
     * @return T
     */
    public abstract long getT();

    /**
     * Sets the T value
     *
     * @param t new t
     */
    public abstract void setT(long t);

    /**
     * Gets the value, if result was already found
     *
     * @return resultFound
     */
    public abstract boolean getResultFound();

    /**
     * Sets the resultFound value
     *
     * @param resultFound new resultFound value
     */
    public abstract void setResultFound(boolean resultFound);

    /**
     * Gets the result of the algorithm
     *
     * @return result
     */
    public abstract float getResult();

    /**
     * Sets the result value
     *
     * @param result new result
     */
    public abstract void setResult(float result);

    /**
     * Gets the iteration count
     *
     * @return iteration count
     */
    public abstract int getNumberOfIterations();

    /**
     * Sets the number of iterations
     *
     * @param iterationCount the iteration count
     */
    public abstract void setNumberOfIterations(int iterationCount);

    /**
     * Closes this adapter and all dependencies
     */
    public abstract void close();

    /**
     * Resets this adapter
     */
    public abstract void reset();

    /**
     * Factory method, initiates adapter by given properties, which hold the adapter name
     *
     * @param properties the properties which hold the adapter name
     *
     * @return the adapter or fake adapter if not found
     */
    public static AbstractRedisAdapter factory(AppProperties properties) {
        String redisAdapterName = properties.getProperty(PropertyName.REDIS_ADAPTER);
        switch (redisAdapterName) {
            case REDIS_ADAPTER_JEDIS:
                return new JedisRedisAdapter(
                        properties.getProperty(PropertyName.REDIS_HOST),
                        Integer.valueOf(properties.getProperty(PropertyName.REDIS_PORT))
                );
            case REDIS_ADAPTER_FAKE:
                return FakeRedisAdapter.getInstance();
            default:
                return FakeRedisAdapter.getInstance();
        }
    }

    /**
     * Set the redis password
     *
     * @param redisPassword the redis password to set
     */
    public static void setRedisPassword(String redisPassword) {
        AbstractRedisAdapter.redisPassword = redisPassword;
    }

    /**
     * Get the redis password
     *
     * @return the redis password if set
     */
    public static String getRedisPassword() {
        return redisPassword;
    }

}