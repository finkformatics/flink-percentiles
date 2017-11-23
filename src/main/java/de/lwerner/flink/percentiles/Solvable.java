package de.lwerner.flink.percentiles;

/**
 * Defines a solvable algorithm
 *
 * @author Lukas Werner
 */
public interface Solvable {

    /**
     * Solves the algorithm, using the field source as data source and the field sink as data sink.
     *
     * @throws Exception if anything goes wrong
     */
    void solve() throws Exception;

}