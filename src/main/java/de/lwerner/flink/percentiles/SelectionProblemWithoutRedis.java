package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractSelectionProblem;
import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.functions.CalculateWeightedMedianGroupReduceFunction;
import de.lwerner.flink.percentiles.functions.join.*;
import de.lwerner.flink.percentiles.math.QuickSelect;
import de.lwerner.flink.percentiles.model.ResultReport;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An algorithm for the selection problem. The ladder is the problem to find the kth smallest element in an unordered
 * set of elements. Sequentially, this is as easy as bringing the elements in order and getting the kth element,
 * in a distributed or parallel environment, this has to be done differently, when focus lies on performance.
 *
 * @author Lukas Werner
 */
public class SelectionProblemWithoutRedis extends AbstractSelectionProblem {

    /**
     * Threshold for sequential computation
     */
    public static final long VALUE_COUNT_THRESHOLD = 1000000;

    /**
     * Should we use the sink?
     */
    private boolean useSink;

    /**
     * Store result
     */
    private float result;

    /**
     * SelectionProblemWithoutRedis constructor, sets the required values
     *
     * @param source the data source
     * @param sink   the data sink
     * @param k      the ranks
     * @param t      serial computation threshold
     */
    public SelectionProblemWithoutRedis(SourceInterface source, SinkInterface sink, long[] k, long t) {
        this(source, sink, k, t, true);
    }

    /**
     * SelectionProblemWithoutRedis constructor, sets the required values, utilizes useSink parameter
     *
     * @param source the data source
     * @param sink the data sink
     * @param k the ranks
     * @param t serial computation threshold
     * @param useSink use the sink directly
     */
    public SelectionProblemWithoutRedis(SourceInterface source, SinkInterface sink, long[] k, long t, boolean useSink) {
        super(source, sink, k, t);

        this.useSink = useSink;
    }

    /**
     * Get result
     *
     * @return result
     */
    public float getResult() {
        return result;
    }

    @Override
    public void solve() throws Exception {
        IterativeDataSet<Tuple3<Float, Long, Long>> initial = getSource().getDataSet()
                .map(new InputToTupleMapFunction(getFirstK(), getSource().getCount()))
                .iterate(1000);

        DataSet<Tuple3<Float, Long, Long>> mediansCountsAndN = initial
                .partitionByHash(0)
                .sortPartition(0, Order.ASCENDING)
                .mapPartition(new MedianAndCountMapPartitionFunction());

        DataSet<Tuple2<Float, Float>> mediansAndWeights = mediansCountsAndN
                .map(new CalculateWeightsMapFunction());

        DataSet<Tuple1<Float>> weightedMedian = mediansAndWeights
                .reduceGroup(new CalculateWeightedMedianGroupReduceFunction());

        DataSet<Tuple5<Long, Long, Long, Long, Long>> leg = initial
                .map(new CalculateLessEqualAndGreaterMapFunction())
                .withBroadcastSet(weightedMedian, "weightedMedian")
                .reduce(new CalculateLessEqualAndGreaterReduceFunction());

        DataSet<Tuple5<Boolean, Boolean, Float, Long, Long>> decisionBase = leg
                .map(new DecideWhatToDoMapFunction())
                .withBroadcastSet(weightedMedian, "weightedMedian");

        DataSet<Tuple3<Float, Long, Long>> iteration = initial
                .filter(new DiscardValuesFlatMapFunction())
                .withBroadcastSet(decisionBase, "decisionBase")
                .withBroadcastSet(weightedMedian, "weightedMedian");

        // TODO: Problem: How to conditionally reduce to 1 element, if the weighted median is the found result

        DataSet<Tuple5<Boolean, Boolean, Float, Long, Long>> terminationCriterion = decisionBase
                .filter(new TerminationCriterionFilterFunction(VALUE_COUNT_THRESHOLD));

        DataSet<Tuple3<Float, Long, Long>> remaining = initial.closeWith(iteration, terminationCriterion);

        remaining.print();

//        List<Tuple3<Float, Long, Long>> remainingValues = remaining.collect();
//
//        List<Float> values = remainingValues.stream().map(t -> t.f0).collect(Collectors.toList());
//        long k = remainingValues.get(0).f1;

        // TODO: Catch the case if we have an empty list

//        System.out.println(remainingValues.get(0));
//        System.out.println(k);
//
//        QuickSelect quickSelect = new QuickSelect();
//        result = quickSelect.select(values, (int)k - 1);
//
//        if (useSink) {
//            ResultReport resultReport = new ResultReport();
//            resultReport.setK(getK());
//            resultReport.setResults(new float[]{result});
//
//            // Sink for the result
//            getSink().processResult(resultReport);
//        }
    }

    /**
     * The main application method, fetches execution environment, generates random values and executes the main
     * algorithm
     *
     * @param args Currently no supported program arguments
     *
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        long k = Long.valueOf(params.getRequired("k"));

        SelectionProblemWithoutRedis algorithm = factory(SelectionProblemWithoutRedis.class, params, new long[]{k});
        algorithm.solve();
    }

}