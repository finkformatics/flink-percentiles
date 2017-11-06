package de.lwerner.flink.percentiles.model;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Class DecisionModel
 *
 * Defined to not have really big and annoying tuples.
 *
 * @author Lukas Werner
 */
public class DecisionModel extends Tuple5<Boolean, Boolean, Float, Long, Long> {

    /**
     * Default constructor
     */
    public DecisionModel() {
        super();
    }

    /**
     * Constructor, sets values
     *
     * @param foundResult if we found one
     * @param keepLess if we should keep less
     * @param result the result
     * @param k the value for k
     * @param n the value for n
     */
    public DecisionModel(boolean foundResult, boolean keepLess, float result, long k, long n) {
        super(foundResult, keepLess, result, k, n);
    }

    /**
     * Check if we found result
     *
     * @return true, if we found one
     */
    public boolean isFoundResult() {
        return f0;
    }

    /**
     * Set value for foundResult
     *
     * @param foundResult true, if we found one
     */
    public void setFoundResult(boolean foundResult) {
        f0 = foundResult;
    }

    /**
     * Check if we should keep the less values
     *
     * @return true, if we should keep less
     */
    public boolean isKeepLess() {
        return f1;
    }

    /**
     * Set value for keepLess
     *
     * @param keepLess true, if we should keep less
     */
    public void setKeepLess(boolean keepLess) {
        f1 = keepLess;
    }

    /**
     * Get the result
     *
     * @return the result
     */
    public float getResult() {
        return f2;
    }

    /**
     * Set the result
     *
     * @param result the result
     */
    public void setResult(float result) {
        f2 = result;
    }

    /**
     * Get the current value for k
     *
     * @return the current k
     */
    public long getK() {
        return f3;
    }

    /**
     * Set k
     *
     * @param k the new value for k
     */
    public void setK(long k) {
        f3 = k;
    }

    /**
     * Get the current value for n
     *
     * @return the current n
     */
    public long getN() {
        return f4;
    }

    /**
     * Set n
     *
     * @param n the new value for n
     */
    public void setN(long n) {
        f4 = n;
    }

}