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
     * The result value
     */
    private DataSet<Tuple1<Float>> solution;

    /**
     * The actual result value
     */
    private float value;

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

    /**
     * Get the value
     *
     * @return the value
     */
    public float getValue() {
        return value;
    }

    /**
     * Set the value
     *
     * @param value the value to set
     */
    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("(%d, %d, %d, %f)", p, k, t, value);
    }

}