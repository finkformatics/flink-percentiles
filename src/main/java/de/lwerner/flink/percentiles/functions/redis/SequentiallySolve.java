package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Sequentially solve the remaining problem.
 *
 * @author Lukas Werner
 */
public class SequentiallySolve extends RichMapPartitionFunction<Tuple1<Float>, Float> {

    /**
     * Redis adapter for accessing redis values
     */
    private AbstractRedisAdapter redisAdapter;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAdapter = AbstractRedisAdapter.factory(AppProperties.getInstance());
    }

    @Override
    public void mapPartition(Iterable<Tuple1<Float>> values, Collector<Float> out) throws Exception {
        if (redisAdapter.getResultFound()) {
            return;
        }

        long k = redisAdapter.getNthK(1);
        int n = 1;

        for (Tuple1<Float> t: values) {
            if (n++ == k) {
                out.collect(t.f0);
                break;
            }
        }
    }

}