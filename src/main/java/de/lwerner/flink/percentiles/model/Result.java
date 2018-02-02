package de.lwerner.flink.percentiles.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Class Result
 * <p>
 * Holds information about the solved algorithm.
 *
 * @author Lukas Werner
 */
public class Result {

    /**
     * The input p value
     */
    private int p;

    /**
     * The input k value
     */
    private long k;

    /**
     * The input t value
     */
    private long t;

    /**
     * Result value
     */
    private float result;

    /**
     * The result value
     */
    private DataSet<Tuple1<Float>> solution;

    /**
     * Get the t value
     *
     * @return the t value
     */
    public long getT() {
        return t;
    }

    /**
     * Set the value for t
     *
     * @param t value for t
     */
    public void setT(long t) {
        this.t = t;
    }

    /**
     * Get p value
     *
     * @return p value
     */
    public int getP() {
        return p;
    }

    /**
     * Set p value
     *
     * @param p p value
     */
    public void setP(int p) {
        this.p = p;
    }

    /**
     * Get k value
     *
     * @return k value
     */
    public long getK() {
        return k;
    }

    /**
     * Set k value
     *
     * @param k k value
     */
    public void setK(long k) {
        this.k = k;
    }

    /**
     * Get result
     *
     * @return result
     */
    public float getResult() {
        return result;
    }

    /**
     * Set result
     *
     * @param result result
     */
    public void setResult(float result) {
        this.result = result;
    }

    /**
     * Get solution data set
     *
     * @return the solution data set
     */
    public DataSet<Tuple1<Float>> getSolution() {
        return solution;
    }

    /**
     * Set solution data set
     *
     * @param solution solution data set to set
     */
    public void setSolution(DataSet<Tuple1<Float>> solution) {
        this.solution = solution;
    }

}