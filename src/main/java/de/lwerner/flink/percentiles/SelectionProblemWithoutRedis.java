package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.functions.join.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;

import java.util.*;

/**
 * An algorithm for the selection problem. The ladder is the problem to find the kth smallest element in an unordered
 * set of elements. Sequentially, this is as easy as bringing the elements in order and getting the kth element,
 * in a distributed or parallel environment, this has to be done differently, when focus lies on performance.
 *
 * @author Lukas Werner
 */
public class SelectionProblemWithoutRedis {

    /**
     * The threshold for value count. If we have at most this number of elements, we can calculate sequentially
     */
    public static final int VALUE_COUNT_THRESHOLD = 250000;

    /**
     * The main application method, fetches execution environment, generates random values and executes the main
     * algorithm
     *
     * @param args Currently no supported program arguments
     *
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Double> values = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < 10000000; i++) {
            values.add(rnd.nextDouble());
        }

        int k = 1000000;
        int n = values.size();

        IterativeDataSet<Tuple3<Double, Integer, Integer>> initial = env.fromCollection(values)
                .map(new InputToTupleMapFunction(k, n))
                .iterate(1000);

        DataSet<Tuple3<Double, Integer, Integer>> mediansCountsAndN = initial
                .partitionByHash(0)
                .sortPartition(0, Order.ASCENDING)
                .mapPartition(new MedianAndCountMapPartitionFunction());

        DataSet<Tuple2<Double, Double>> mediansAndWeights = mediansCountsAndN
                .map(new CalculateWeightsMapFunction());

        DataSet<Tuple1<Double>> weightedMedian = mediansAndWeights
                .reduceGroup(new CalculateWeightedMedianGroupReduceFunction());

        DataSet<Tuple5<Integer, Integer, Integer, Integer, Integer>> leg = initial
                .map(new CalculateLessEqualAndGreaterMapFunction())
                .withBroadcastSet(weightedMedian, "weightedMedian")
                .reduce(new CalculateLessEqualAndGreaterReduceFunction());

        DataSet<Tuple5<Boolean, Boolean, Double, Integer, Integer>> decisionBase = leg
                .map(new DecideWhatToDoMapFunction())
                .withBroadcastSet(weightedMedian, "weightedMedian");

        DataSet<Tuple3<Double, Integer, Integer>> iteration = initial
                .flatMap(new DiscardValuesFlatMapFunction())
                .withBroadcastSet(decisionBase, "decisionBase")
                .withBroadcastSet(weightedMedian, "weightedMedian");

        // TODO: Problem: How to conditionally reduce to 1 element, if the weighted median is the found result

        DataSet<Tuple5<Boolean, Boolean, Double, Integer, Integer>> terminationCriterion = decisionBase
                .filter(new TerminationCriterionFilterFunction());

        DataSet<Tuple3<Double, Integer, Integer>> remaining = initial.closeWith(iteration, terminationCriterion);

        List<Tuple3<Double, Integer, Integer>> remainingValues = remaining.collect();

        // TODO: Catch the case if we have an empty list

        remainingValues.sort(Comparator.comparingDouble(o -> o.f0));

        double result = remainingValues.get(remainingValues.get(0).f1 - 1).f0;

        System.out.println(result);
    }

}