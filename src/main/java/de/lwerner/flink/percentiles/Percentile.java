package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractPercentile;
import de.lwerner.flink.percentiles.data.*;
import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Class Percentile
 *
 * Calculates a certain percentile over a huge data set using the distributed selection problem algorithm.
 *
 * @author Lukas Werner
 */
public class Percentile extends AbstractPercentile {

    /**
     * Selection problem solver
     */
    private SelectionProblem selectionProblem;

    /**
     * Percentile constructor. Sets all the required values and calculates k from p.
     *
     * @param source data source
     * @param sink data sink
     * @param p percentile
     * @param t threshold
     */
    public Percentile(SourceInterface source, SinkInterface sink, int p, long t) {
        super(source, sink, p, t);

        float np = source.getCount() / 100f;
        setK((int)Math.ceil(np * p));

        selectionProblem = new SelectionProblem(source, sink, getK(), t, false);
    }

    @Override
    public void solve() throws Exception {
        selectionProblem.solve();

        Result result = selectionProblem.getResult();
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

        Percentile algorithm = factory(Percentile.class, params, p);
        algorithm.solve();
    }

}