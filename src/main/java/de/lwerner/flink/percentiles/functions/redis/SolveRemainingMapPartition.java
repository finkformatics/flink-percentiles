package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.model.RedisCredentials;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Function for solving the remaining problem
 *
 * @author Lukas Werner
 */
public class SolveRemainingMapPartition extends RichMapPartitionFunction<Tuple1<Float>, Tuple1<Float>> {

    /**
     * Redis adapter for accessing redis values
     */
    private AbstractRedisAdapter redisAdapter;

    /**
     * Redis connection info
     */
    private RedisCredentials redisCredentials;

    /**
     * Constructor to set the redis credentials
     *
     * @param redisCredentials the redis credentials
     */
    public SolveRemainingMapPartition(RedisCredentials redisCredentials) {
        this.redisCredentials = redisCredentials;
    }

    @Override
    public void open(Configuration parameters) {
        redisAdapter = AbstractRedisAdapter.factory(redisCredentials);
    }

    @Override
    public void close() {
        redisAdapter.close();
    }

    @Override
    public void mapPartition(Iterable<Tuple1<Float>> values, Collector<Tuple1<Float>> out) {
        List<Tuple1<Float>> valuesList = new ArrayList<>();

        values.forEach(valuesList::add);

        if (redisAdapter.getResultFound()) {
            // Result was found already, just put it into the collector
            out.collect(new Tuple1<>(redisAdapter.getResult()));
        } else {
            if (valuesList.isEmpty()) {
                throw new IllegalStateException("The remaining elements should never be empty! Please check the code!");
            }

            long k = redisAdapter.getK();
            if (valuesList.size() < k) {
                throw new IllegalStateException("The remaining elements are less than k. This should never happen! Please check the code! Remaining size: " + valuesList.size() + ", k: " + k);
            }

            out.collect(valuesList.get((int)k - 1));
        }
    }
}