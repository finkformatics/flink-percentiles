package de.lwerner.flink.percentiles.redis;

/**
 * A fake redis adapter, which simulates the behaviour on single node environments
 */
public class FakeRedisAdapter extends AbstractRedisAdapter {

    /**
     * Singleton instance
     */
    private static FakeRedisAdapter instance;

    /**
     * Easy holding of n
     */
    private long n;
    /**
     * k
     */
    private long k;
    /**
     * t
     */
    private long t;
    /**
     * resultFound
     */
    private boolean resultFound;
    /**
     * result
     */
    private float result;

    @Override
    public long getN() {
        return n;
    }

    @Override
    public void setN(long n) {
        this.n = n;
    }

    @Override
    public long getK() {
        return k;
    }

    @Override
    public void setK(long k) {
        this.k = k;
    }

    @Override
    public long getT() {
        return t;
    }

    @Override
    public void setT(long t) {
        this.t = t;
    }

    @Override
    public boolean getResultFound() {
        return resultFound;
    }

    @Override
    public void setResultFound(boolean resultFound) {
        this.resultFound = resultFound;
    }

    @Override
    public float getResult() {
        return result;
    }

    @Override
    public void setResult(float result) {
        this.result = result;
    }

    @Override
    public void close() {
        // Do nothing
    }

    /**
     * Singleton access method
     *
     * @return the instance
     */
    public static FakeRedisAdapter getInstance() {
        if (instance == null) {
            instance = new FakeRedisAdapter();
        }

        return instance;
    }

}
