package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractPercentile;
import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.model.ResultReport;
import de.lwerner.flink.percentiles.timeMeasurement.Timer;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Calculates an approximative percentile over a huge amount of values.
 *
 * @author Lukas Werner
 */
public class ApproximativePercentile extends AbstractPercentile {

    /**
     * Approximative selection problem solver
     */
    private ApproximativeSelectionProblem approximativeSelectionProblem;

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

        float np = source.getCount() / 100f;
        setK((int)(np * p));

        approximativeSelectionProblem = new ApproximativeSelectionProblem(source, sink, new long[]{getK()}, t, false);
    }

    /**
     * Get the selection problem solver
     *
     * @return algorithm solver
     */
    public ApproximativeSelectionProblem getApproximativeSelectionProblem() {
        return approximativeSelectionProblem;
    }

    @Override
    public void solve() throws Exception {
        approximativeSelectionProblem.solve();

        float result = approximativeSelectionProblem.getResult();

        Timer timer = approximativeSelectionProblem.getTimer();

        ResultReport resultReport = new ResultReport();
        resultReport.setTimerResults(timer.getTimerResults());
        resultReport.setResults(new float[]{result});
        resultReport.setP(new int[]{getP()});
        resultReport.setK(new long[]{getK()});

        getSink().processResult(resultReport);
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
        long s = Long.valueOf(params.getRequired("sample-size"));

        ApproximativePercentile algorithm = factory(ApproximativePercentile.class, params, p);
        algorithm.getApproximativeSelectionProblem().setSampleSize(s);
        algorithm.solve();
    }

}