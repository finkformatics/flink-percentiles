package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Calculates an approximative percentile over a huge amount of values.
 *
 * @author Lukas Werner
 */
public class ApproximativePercentile extends AbstractPercentile {

    /**
     * ApproximativePercentile constructor, sets the required values, here t is interpreted as the sample size.
     *
     * @param source the data source
     * @param sink   the data sink
     * @param p      the percentage value
     * @param t      serial computation threshold
     */
    protected ApproximativePercentile(SourceInterface source, SinkInterface sink, int p, long t) {
        super(source, sink, p, t);
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

        int p = Integer.valueOf(params.getRequired("p"));

        ApproximativePercentile algorithm = factory(ApproximativePercentile.class, params, p);
        algorithm.solve();
    }

}