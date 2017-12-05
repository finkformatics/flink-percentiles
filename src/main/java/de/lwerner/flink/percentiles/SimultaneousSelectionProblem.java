package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractSelectionProblem;
import de.lwerner.flink.percentiles.data.*;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Class SimultaneousSelectionProblem
 *
 * Implements the simultaneous selection problem algorithm in flink
 *
 * @author Lukas Werner
 */
public class SimultaneousSelectionProblem extends AbstractSelectionProblem {

    /**
     * SimultaneousSelectionProblem constructor, sets the required values
     *
     * @param source the data source
     * @param sink the data sink
     * @param k the ranks
     * @param t serial computation threshold
     */
    public SimultaneousSelectionProblem(SourceInterface source, SinkInterface sink, long[] k, long t) {
        super(source, sink, k, t);
    }

    @Override
    public void solve() throws Exception {

    }

    /**
     * The main application method, fetches execution environment, generates random values and executes the main
     * algorithm
     *
     * @param args the command line arguments
     *
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String kStr = params.getRequired("k");
        String[] kStrSplit = kStr.split(",");

        long[] k = new long[kStrSplit.length];
        for (int i = 0; i < kStrSplit.length; i++) {
            k[i] = Long.valueOf(kStrSplit[i]);
        }

        SimultaneousSelectionProblem algorithm = factory(SimultaneousSelectionProblem.class, params, k);
        algorithm.solve();
    }

}