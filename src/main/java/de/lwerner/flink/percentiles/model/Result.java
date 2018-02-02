package de.lwerner.flink.percentiles.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.HashMap;
import java.util.Map;

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
    private Integer p;

    /**
     * The input k value
     */
    private Long k;

    /**
     * The input t value
     */
    private Long t;

    /**
     * The number of iterations needed
     */
    private Integer numberOfIterations;

    /**
     * Results from timer
     */
    private HashMap<String, Long> timerResults;

    /**
     * Result value
     */
    private Float result;

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
     * Get the number of iterations
     *
     * @return number of iterations
     */
    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    /**
     * Set the number of iterations
     *
     * @param numberOfIterations number of iterations
     */
    public void setNumberOfIterations(int numberOfIterations) {
        this.numberOfIterations = numberOfIterations;
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
     * Get timer results
     *
     * @return timer results
     */
    public HashMap<String, Long> getTimerResults() {
        return timerResults;
    }

    /**
     * Set timer results
     *
     * @param timerResults timer results
     */
    public void setTimerResults(HashMap<String, Long> timerResults) {
        this.timerResults = timerResults;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Result Report:\n");
        if (p != null) {
            sb.append("p = ");
            sb.append(p);
            sb.append('\n');
        }

        if (k != null) {
            sb.append("k = ");
            sb.append(k);
            sb.append('\n');
        }

        if (result != null) {
            sb.append("result = ");
            sb.append(result);
            sb.append('\n');
        }

        if (timerResults != null) {
            for (Map.Entry<String, Long> entry: timerResults.entrySet()) {
                sb.append("Timer '");
                sb.append(entry.getKey());
                sb.append("': ");
                sb.append((entry.getValue() / 1000.0));
                sb.append("s\n");
            }
        }

        if (t > 0) {
            sb.append("Per node value count threshold: ");
            sb.append(t);
            sb.append('\n');
        }

        if (numberOfIterations > 0) {
            sb.append("Number of iterations: ");
            sb.append(numberOfIterations);
            sb.append('\n');
        }

        return sb.toString();
    }
}