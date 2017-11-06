package de.lwerner.flink.percentiles.redis;

import de.lwerner.flink.percentiles.util.JedisHelper;
import redis.clients.jedis.Jedis;

/**
 * Concrete Jedis access adapter class, uses JedisHelper to get and set values
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

    @Override
    public long getN() {
        return JedisHelper.getN(jedis);
    }

    @Override
    public void setN(long n) {
        JedisHelper.setN(jedis, n);
    }

    @Override
    public long getK() {
        return JedisHelper.getK(jedis);
    }

    @Override
    public void setK(long k) {
        JedisHelper.setK(jedis, k);
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
    public void close() {
        jedis.close();
    }
}