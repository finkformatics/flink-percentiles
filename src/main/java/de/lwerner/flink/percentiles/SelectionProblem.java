package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.data.*;
import de.lwerner.flink.percentiles.functions.redis.*;
import de.lwerner.flink.percentiles.model.DecisionModel;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * An algorithm for the selection problem. The ladder is the problem to find the kth smallest element in an unordered
 * set of elements. Sequentially, this is as easy as bringing the elements in order and getting the kth element,
 * in a distributed or parallel environment, this has to be done differently, when focus lies on performance.
 *
 * @author Lukas Werner
 */
public class SelectionProblem extends AbstractSelectionProblem {

    /**
     * SelectionProblem constructor, sets the required values
     *
     * @param source the data source
     * @param sink the data sink
     * @param k the ranks
     * @param t serial computation threshold
     */
    public SelectionProblem(SourceInterface source, SinkInterface sink, long[] k, long t) {
        super(source, sink, k, t);
    }

    /**
     * Solves the selection problem
     *
     * @throws Exception if anything goes wrong
     */
    public void solve() throws Exception {
        // Holds important information just as how to connect to redis
        AppProperties properties = AppProperties.getInstance();

        // Create a redis adapter
        AbstractRedisAdapter redisAdapter = AbstractRedisAdapter.factory(properties);

        // Initiate the values on redis
        redisAdapter.setK(getFirstK());
        redisAdapter.setN(getSource().getCount());
        redisAdapter.setT(getT());

        // Start iteration on initial data set
        IterativeDataSet<Tuple1<Float>> initial = getSource()
                .getDataSet()
                .iterate(1000);

        // Create partitions, calculate medians and count values on each partition
        DataSet<Tuple2<Float, Long>> mediansCountsAndN = initial
                .partitionByHash(0)
                .sortPartition(0, Order.ASCENDING)
                .mapPartition(new MedianAndCountMapPartitionFunction());

        // Calculate weights (percentage part of total values)
        DataSet<Tuple2<Float, Float>> mediansAndWeights = mediansCountsAndN
                .map(new CalculateWeightsMapFunction());

        // Calculate the weighted median
        DataSet<Tuple1<Float>> weightedMedian = mediansAndWeights
                .reduceGroup(new CalculateWeightedMedianGroupReduceFunction());

        // Count how much values are below (l), equal (e) or higher (g) than the weighted median
        DataSet<Tuple3<Long, Long, Long>> leg = initial
                .map(new CalculateLessEqualAndGreaterMapFunction())
                .withBroadcastSet(weightedMedian, "weightedMedian")
                .reduce(new CalculateLessEqualAndGreaterReduceFunction());

        // Decision after each iteration:
        // - k > |less values| && k <= |less values| + |equal values|: Found result!
        // - k <= |less values|: k must be below weighted median (discard any values, which are equal and greater W)
        // - k > |less values| + |equal values|: k must be higher than weighted median (discard equal and less)
        DataSet<DecisionModel> decisionBase = leg
                .map(new DecideWhatToDoMapFunction());

        // Actually discard the values by the decision base
        DataSet<Tuple1<Float>> iteration = initial
                .filter(new DiscardValuesFilterFunction())
                .withBroadcastSet(decisionBase, "decisionBase")
                .withBroadcastSet(weightedMedian, "weightedMedian");

        // Clear data set, if we're finished
        DataSet<DecisionModel> terminationCriterion = decisionBase
                .filter(new TerminationCriterionFilterFunction());

        // Iterate, until finish condition is met
        DataSet<Tuple1<Float>> remaining = initial.closeWith(iteration, terminationCriterion);

        // Data sink for the remaining values
        List<Float> remainingValues = remaining
                .map(new RemainingValuesMapFunction())
                .filter(new RemainingValuesFilterFunction())
                .collect();

        // Get the current k value from redis
        long remainingK = redisAdapter.getK();

        // Calculate sequentially
        float result;
        if (!remainingValues.isEmpty()) {
            Collections.sort(remainingValues);
            result = remainingValues.get((int)remainingK - 1);
        } else {
            result = redisAdapter.getResult();
        }

        redisAdapter.close();

        // Sink for the result
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

        long k = Long.valueOf(params.getRequired("k"));

        SelectionProblem algorithm = factory(SelectionProblem.class, params, new long[]{k});
        algorithm.solve();
    }

}