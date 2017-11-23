package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

/**
 * Class SequentialPercentile
 *
 * Calculates a certain percentile sequentially.
 *
 * @author Lukas Werner
 */
public class SequentialPercentile extends AbstractPercentile {

    /**
     * Percentile constructor. Sets all the required values and calculates k from p.
     *
     * @param source data source
     * @param sink data sink
     * @param p percentile
     * @param t threshold
     */
    public SequentialPercentile(SourceInterface source, SinkInterface sink, int p, long t) {
        super(source, sink, p, t);

        float np = source.getCount() / 100f; // Might seem too complex, but first lower the number, then multiply
        setK((int)(np * p));
    }

    @Override
    public void solve() throws Exception {
        List<Float> values = getSource().getValues();

        getTimer().startTimer();
        float result = select(values, values.size() - 1, getK());
        getTimer().stopTimer();

        ResultReport resultReport = new ResultReport();
        resultReport.setTimerResults(getTimer().getTimerResults());
        resultReport.setResults(new float[]{result});
        resultReport.setP(new int[]{getP()});
        resultReport.setK(new long[]{getK()});

        getSink().processResult(resultReport);
    }

    /**
     * Quicksort like selection algorithm.
     *
     * @param list the values list
     * @param right the right bound
     * @param k the rank
     *
     * @return the result
     */
    private float select(List<Float> list, int right, int k) {
        int left = 0;
        while (true) {
            if (left == right) {
                return list.get(left);
            }

            int pivotIndex = left + (int) Math.floor((right - left + 1) / 2.0);
            pivotIndex = partition(list, left, right, pivotIndex);

            if (k == pivotIndex) {
                return list.get(k);
            } else if (k < pivotIndex) {
                right = pivotIndex - 1;
            } else {
                left = pivotIndex + 1;
            }
        }
    }

    /**
     * Method to partition the list
     *
     * @param list the values list
     * @param left the left bound
     * @param right the right bound
     * @param pivotIndex the current pivot index
     *
     * @return the new pivot index
     */
    private int partition(List<Float> list, int left, int right, int pivotIndex) {
        float pivotValue = list.get(pivotIndex);
        swap(list, pivotIndex, right);
        int storeIndex = left;

        for (int i = left; i < right; i++) {
            if (list.get(i) < pivotValue) {
                swap(list, storeIndex, i);
                storeIndex++;
            }
        }

        swap(list, right, storeIndex);

        return storeIndex;
    }

    /**
     * Swap method to exchange to values in list by their indexes
     *
     * @param list the values list
     * @param index1 the first index
     * @param index2 the second index
     */
    private void swap(List<Float> list, int index1, int index2) {
        float temp = list.get(index1);
        list.set(index1, list.get(index2));
        list.set(index2, temp);
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

        SequentialPercentile algorithm = factory(SequentialPercentile.class, params, p);
        algorithm.solve();
    }

}