package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractAlgorithm;
import de.lwerner.flink.percentiles.algorithm.AbstractPercentile;
import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.math.QuickSelect;
import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

/**
 * Class SequentialPercentile
 *
 * Calculates a certain percentile sequentially.
 *
 * @author Lukas Werner
 */
public class SequentialPercentile extends AbstractAlgorithm {

    private int p;
    private int k;

    /**
     * Percentile constructor. Sets all the required values and calculates k from p.
     *
     * @param source data source
     * @param sink data sink
     * @param p percentile
     * @param t threshold
     */
    public SequentialPercentile(SourceInterface source, SinkInterface sink, int p, long t) {
        super(source, sink);

        this.p = p;

        float np = source.getCount() / 100f; // Might seem too complex, but first lower the number, then multiply
        setK((int)Math.ceil(np * p));
    }

    public int getP() {
        return p;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    @Override
    public void solve() throws Exception {
        List<Float> values = getSource().getValues();

        QuickSelect quickSelect = new QuickSelect();

        getTimer().startTimer();
        float resultValue = quickSelect.select(values, getK() - 1);
        getTimer().stopTimer();

        Result result = new Result();
        result.setP(getP());
        result.setK(getK());
        result.setValue(resultValue);

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

        SequentialPercentile algorithm = AbstractPercentile.factory(SequentialPercentile.class, params, p);
        algorithm.solve();
    }

}