package de.lwerner.flink.percentiles;

import java.util.HashMap;
import java.util.Map;

/**
 * Class ResultReport
 *
 * Holds information about the solved algorithm.
 *
 * @author Lukas Werner
 */
public class ResultReport {

    /**
     * The input p values
     */
    private int[] p;

    /**
     * The input k values
     */
    private long[] k;

    /**
     * Results from timer
     */
    private HashMap<String, Long> timerResults;

    /**
     * The result values
     */
    private float[] results;

    /**
     * Get p values
     *
     * @return p values
     */
    public int[] getP() {
        return p;
    }

    /**
     * Set p values
     *
     * @param p p values
     */
    public void setP(int[] p) {
        this.p = p;
    }

    /**
     * Get k values
     *
     * @return k values
     */
    public long[] getK() {
        return k;
    }

    /**
     * Set k values
     *
     * @param k k values
     */
    public void setK(long[] k) {
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
     * Get results
     *
     * @return results
     */
    public float[] getResults() {
        return results;
    }

    /**
     * Set results
     *
     * @param results results
     */
    public void setResults(float[] results) {
        this.results = results;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Result Report:\n");
        if (p != null && p.length > 0) {
            for (int i = 0; i < p.length; i++) {
                sb.append("p[");
                sb.append(i);
                sb.append("] = ");
                sb.append(p[i]);
                sb.append('\n');
            }
        }

        if (k != null && k.length > 0) {
            for (int i = 0; i < k.length; i++) {
                sb.append("k[");
                sb.append(i);
                sb.append("] = ");
                sb.append(k[i]);
                sb.append('\n');
            }
        }

        if (results != null && results.length > 0) {
            for (int i = 0; i < results.length; i++) {
                sb.append("result[");
                sb.append(i);
                sb.append("] = ");
                sb.append(results[i]);
                sb.append('\n');
            }
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

        return sb.toString();
    }
}