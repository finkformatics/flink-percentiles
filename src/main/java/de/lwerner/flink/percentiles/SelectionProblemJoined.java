package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.functions.CalculateWeightedMedianGroupReduceFunction;
import de.lwerner.flink.percentiles.functions.join.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import java.util.concurrent.TimeUnit;

/**
 * An algorithm for the selection problem. The ladder is the problem to find the kth smallest element in an unordered
 * set of elements. Sequentially, this is as easy as bringing the elements in order and getting the kth element,
 * in a distributed or parallel environment, this has to be done differently, when focus lies on performance.
 *
 * @author Lukas Werner
 */
public class SelectionProblemJoined {

    private String path;
    private long k;
    private long count;
    private long countThreshold;

    public void solve() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Float> dataSet = env.readFileOfPrimitives(path, Float.class);

        IterativeDataSet<Tuple3<Float, Long, Long>> initial = dataSet
                .map(new de.lwerner.flink.percentiles.functions.redis.InputToTupleMapFunction())
                .map(new InputToTupleMapFunction(k, count))
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

        DataSet<Tuple5<Boolean, Boolean, Float, Long, Long>> terminationCriterion = decisionBase
                .filter(new TerminationCriterionFilterFunction(getCountThreshold()));

        DataSet<Tuple3<Float, Long, Long>> remaining = initial.closeWith(iteration, terminationCriterion);

        remaining.writeAsCsv(path + "_result", FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult jobExecutionResult = remaining.getExecutionEnvironment().execute();
        System.out.println(jobExecutionResult.getNetRuntime(TimeUnit.SECONDS));

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
//            Result resultReport = new Result();
//            resultReport.setK(getK());
//            resultReport.setResults(new float[]{result});
//
//            // Sink for the result
//            getSink().processResult(resultReport);
//        }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getK() {
        return k;
    }

    public void setK(long k) {
        this.k = k;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    private long getCountThreshold() {
        return countThreshold;
    }

    private void setCountThreshold(long countThreshold) {
        this.countThreshold = countThreshold;
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

        long n = Long.valueOf(params.getRequired("count"));
        if (n == 0) {
            throw new IllegalArgumentException("If you're using the generator data source, please provide a value count: --count <num>");
        }

        long t = Long.valueOf(params.get("t", "1000"));

        if (t < 100) {
            throw new IllegalArgumentException("Please provide a serial threshold of at least 100");
        }

        String path = params.getRequired("input-path");
        if (!path.startsWith("hdfs://")) {
            throw new IllegalArgumentException("Path must start with hdfs://");
        }

        SelectionProblemJoined algorithm = new SelectionProblemJoined();

        algorithm.setPath(path);
        algorithm.setK(k);
        algorithm.setCount(n);
        algorithm.setCountThreshold(t);
        algorithm.solve();
    }

}