package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.functions.redis.*;
import de.lwerner.flink.percentiles.model.DecisionModel;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Collections;
import java.util.List;

/**
 * An optimization for the multi selection problem by reducing the data set
 *
 * @author Lukas Werner
 */
public class SetReductionMultiSelectionProblem extends AbstractSelectionProblem {

    /**
     * SetReductionMultiSelectionProblem constructor, sets the required values
     *
     * @param source the data source
     * @param sink the data sink
     * @param k the ranks
     * @param t serial computation threshold
     */
    public SetReductionMultiSelectionProblem(SourceInterface source, SinkInterface sink, long[] k, long t) {
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
        redisAdapter.reset();

        for (long kVal: getK()) {
            redisAdapter.addK(kVal);
        }

        redisAdapter.setN(getSource().getCount());
        redisAdapter.setT(getT());

        redisAdapter.close();
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

        String kStr = params.getRequired("k");
        String[] kStrSplit = kStr.split(",");

        long[] k = new long[kStrSplit.length];
        for (int i = 0; i < kStrSplit.length; i++) {
            k[i] = Long.valueOf(kStrSplit[i]);
        }

        SetReductionMultiSelectionProblem algorithm = factory(SetReductionMultiSelectionProblem.class, params, k);
        algorithm.solve();
    }

}