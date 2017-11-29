package de.lwerner.flink.percentiles.redis;

import java.util.ArrayList;

/**
 * A fake redis adapter, which simulates the behaviour on single node environments
 *
 * @author Lukas Werner
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
    private ArrayList<Long> k;
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
    /**
     * iteration count
     */
    private int iterationCount;

    /**
     * Constructor.
     *
     * Initializes k array list
     */
    private FakeRedisAdapter() {
        k = new ArrayList<>();
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public void setN(long n) {
        this.n = n;
    }

    @Override
    public void addK(long k) {
        this.k.add(k);
    }

    @Override
    public long[] getAllK() {
        long[] kValues = new long[k.size()];

        for (int i = 0; i < k.size(); i++) {
            kValues[i] = k.get(i);
        }

        return kValues;
    }

    @Override
    public long getNthK(int n) {
        if (k.size() >= n) {
            return k.get(n - 1);
        }

        return 0L;
    }

    @Override
    public void setNthK(long k, int n) {
        this.k.set(n - 1, k);
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
    public int getNumberOfIterations() {
        return iterationCount;
    }

    @Override
    public void setNumberOfIterations(int iterationCount) {
        this.iterationCount = iterationCount;
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public void reset() {
        setN(0);
        setResult(0);
        setResultFound(false);
        setT(0);
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
