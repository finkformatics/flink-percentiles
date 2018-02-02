package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.algorithm.AbstractSelectionProblem;
import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.functions.CalculateWeightedMedianGroupReduceFunction;
import de.lwerner.flink.percentiles.functions.join.*;
import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * An algorithm for the selection problem. The ladder is the problem to find the kth smallest element in an unordered
 * set of elements. Sequentially, this is as easy as bringing the elements in order and getting the kth element,
 * in a distributed or parallel environment, this has to be done differently, when focus lies on performance.
 *
 * @author Lukas Werner
 */
public class SelectionProblemWithoutRedis extends AbstractSelectionProblem {

    /**
     * Should we use the sink?
     */
    private boolean useSink;

    /**
     * Store result
     */
    private Result result;

    /**
     * SelectionProblemWithoutRedis constructor, sets the required values
     *
     * @param source the data source
     * @param sink   the data sink
     * @param k      the rank
     * @param t      serial computation threshold
     */
    public SelectionProblemWithoutRedis(SourceInterface source, SinkInterface sink, long k, long t) {
        this(source, sink, k, t, true);
    }

    /**
     * SelectionProblemWithoutRedis constructor, sets the required values, utilizes useSink parameter
     *
     * @param source the data source
     * @param sink the data sink
     * @param k the rank
     * @param t serial computation threshold
     * @param useSink use the sink directly
     */
    public SelectionProblemWithoutRedis(SourceInterface source, SinkInterface sink, long k, long t, boolean useSink) {
        super(source, sink, k, t);

        this.useSink = useSink;
    }

    /**
     * Get result
     *
     * @return result
     */
    public Result getResult() {
        return result;
    }

    @Override
    public void solve() throws Exception {
        IterativeDataSet<Tuple3<Float, Long, Long>> initial = getSource().getDataSet()
                .map(new InputToTupleMapFunction(getK(), getSource().getCount()))
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

        DataSet<Tuple3<Boolean, Long, Long>> decisionBase = leg
                .map(new DecideWhatToDoMapFunction());

        DataSet<Tuple3<Float, Long, Long>> iteration = initial
                .flatMap(new DiscardValuesFlatMapFunction())
                .withBroadcastSet(decisionBase, "decisionBase")
                .withBroadcastSet(weightedMedian, "weightedMedian");

        // Please note: The advantage of the algorithm, to stop when the result was found already, cannot be applied here

        DataSet<Tuple3<Boolean, Long, Long>> terminationCriterion = decisionBase
                .filter(new TerminationCriterionFilterFunction(getT()));

        DataSet<Tuple3<Float, Long, Long>> remaining = initial.closeWith(iteration, terminationCriterion);

        DataSet<Tuple1<Float>> solution = remaining
                .partitionByHash(0).setParallelism(1)
                .sortPartition(0, Order.ASCENDING).setParallelism(1)
                .mapPartition(new SolveRemainingMapPartition()).setParallelism(1)
                .map((MapFunction<Tuple3<Float, Long, Long>, Tuple1<Float>>) value -> new Tuple1<>(value.f0));

        result = new Result();
        result.setSolution(solution);
        result.setK(getK());
        result.setT(getT());

        if (useSink) {
            getSink().processResult(result);
        }
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

        SelectionProblemWithoutRedis algorithm = factory(SelectionProblemWithoutRedis.class, params, k);
        algorithm.solve();
    }

}