package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractPercentile;
import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Class PercentileWithoutRedis
 *
 * Calculates a certain percentile over a huge data set using the distributed selection problem algorithm.
 *
 * @author Lukas Werner
 */
public class PercentileWithoutRedis extends AbstractPercentile {

    /**
     * Selection problem solver
     */
    private SelectionProblemWithoutRedis selectionProblemWithoutRedis;

    /**
     * Percentile constructor. Sets all the required values and calculates k from p.
     *
     * @param source data source
     * @param sink data sink
     * @param p percentile
     * @param t threshold
     */
    public PercentileWithoutRedis(SourceInterface source, SinkInterface sink, int p, long t) {
        super(source, sink, p, t);

        float np = source.getCount() / 100f;
        setK((int)Math.ceil(np * p));

        selectionProblemWithoutRedis = new SelectionProblemWithoutRedis(source, sink, getK(), t, false);
    }

    @Override
    public void solve() throws Exception {
        selectionProblemWithoutRedis.solve();

        Result result = selectionProblemWithoutRedis.getResult();
        result.setP(getP());

        getSink().processResult(result);
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

        PercentileWithoutRedis algorithm = factory(PercentileWithoutRedis.class, params, p);
        algorithm.solve();
    }

}