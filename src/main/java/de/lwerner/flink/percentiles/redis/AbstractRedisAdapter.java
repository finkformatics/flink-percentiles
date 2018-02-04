package de.lwerner.flink.percentiles.redis;

import de.lwerner.flink.percentiles.model.RedisCredentials;

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
     * Sets a new k value
     *
     * @param k new k value
     */
    public abstract void setK(long k);

    /**
     * Get the value of k
     *
     * @return k
     */
    public abstract long getK();

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
     * @param redisCredentials the redis connection info
     *
     * @return the adapter or fake adapter if not found
     */
    public static AbstractRedisAdapter factory(RedisCredentials redisCredentials) {
        switch (redisCredentials.getAdapter()) {
            case REDIS_ADAPTER_JEDIS:
                JedisRedisAdapter adapter = new JedisRedisAdapter(redisCredentials.getHost(), redisCredentials.getPort());

                if (redisCredentials.getPassword() != null) {
                    adapter.auth(redisCredentials.getPassword());
                }

                return adapter;
            case REDIS_ADAPTER_FAKE:
                return FakeRedisAdapter.getInstance();
            default:
                return FakeRedisAdapter.getInstance();
        }
    }

}