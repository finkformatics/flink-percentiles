package de.lwerner.flink.percentiles.redis;

import de.lwerner.flink.percentiles.util.AppProperties;
import de.lwerner.flink.percentiles.util.PropertyName;

/**
 * Class AbstractRedisAdapter
 *
 * Holds the abstract methods for working with redis in this application and provides a factory method.
 */
public abstract class AbstractRedisAdapter {

    /**
     * The Jedis adapter name
     */
    private static final String REDIS_ADAPTER_JEDIS = "jedis";
    /**
     * The adapter name for a redis simulation
     */
    private static final String REDIS_ADAPTER_FAKE = "fake";

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
     * Gets the k value (rank of the searched number)
     *
     * @return k
     */
    public abstract long getK();

    /**
     * Sets the k value
     *
     * @param k new k
     */
    public abstract void setK(long k);

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
     * Closes this adapter and all dependencies
     */
    public abstract void close();

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

}