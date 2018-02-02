package de.lwerner.flink.percentiles.redis;

import de.lwerner.flink.percentiles.util.JedisHelper;
import redis.clients.jedis.Jedis;

/**
 * Concrete Jedis access adapter class, uses JedisHelper to get and set values
 *
 * @author Lukas Werner
 */
public class JedisRedisAdapter extends AbstractRedisAdapter {

    /**
     * Jedis api
     */
    private Jedis jedis;

    /**
     * Constructor, connects to Redis by using JedisHelper and Jedis itself
     *
     * @param host the redis host
     * @param port the redis port
     */
    public JedisRedisAdapter(String host, int port) {
        jedis = JedisHelper.getJedis(host, port);
    }

    /**
     * Authenticate
     *
     * @param password the password to send
     */
    public void auth(String password) {
        jedis.auth(password);
    }

    @Override
    public long getN() {
        return JedisHelper.getN(jedis);
    }

    @Override
    public void setN(long n) {
        JedisHelper.setN(jedis, n);
    }

    @Override
    public void setK(long k) {
        JedisHelper.setK(jedis, k);
    }

    @Override
    public long getK() {
        return JedisHelper.getK(jedis);
    }

    @Override
    public long getT() {
        return JedisHelper.getT(jedis);
    }

    @Override
    public void setT(long t) {
        JedisHelper.setT(jedis, t);
    }

    @Override
    public boolean getResultFound() {
        return JedisHelper.getResultFound(jedis);
    }

    @Override
    public void setResultFound(boolean resultFound) {
        JedisHelper.setResultFound(jedis, resultFound);
    }

    @Override
    public float getResult() {
        return JedisHelper.getResult(jedis);
    }

    @Override
    public void setResult(float result) {
        JedisHelper.setResult(jedis, result);
    }

    @Override
    public int getNumberOfIterations() {
        return JedisHelper.getNumberOfIterations(jedis);
    }

    @Override
    public void setNumberOfIterations(int iterationCount) {
        JedisHelper.setNumberOfIterations(jedis, iterationCount);
    }

    @Override
    public void close() {
        jedis.close();
    }

    @Override
    public void reset() {
        // Do nothing
    }
}