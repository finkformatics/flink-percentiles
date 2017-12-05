package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.model.RedisCredentials;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Function, which calculates the weights for each partition
 *
 * @author Lukas Werner
 */
public class CalculateWeightsMapFunction extends RichMapFunction<Tuple2<Float, Long>, Tuple2<Float, Float>> {

    /**
     * The number of the currently remaining elements
     */
    private long n;

    /**
     * Redis connection info
     */
    private RedisCredentials redisCredentials;

    /**
     * Constructor to set the redis credentials
     *
     * @param redisCredentials the redis credentials
     */
    public CalculateWeightsMapFunction(RedisCredentials redisCredentials) {
        this.redisCredentials = redisCredentials;
    }

    @Override
    public void open(Configuration parameters) {
        AbstractRedisAdapter redisAdapter = AbstractRedisAdapter.factory(redisCredentials);
        n = redisAdapter.getN();
        redisAdapter.close();
    }

    @Override
    public Tuple2<Float, Float> map(Tuple2<Float, Long> medianCountAndN) {
        return new Tuple2<>(medianCountAndN.f0, medianCountAndN.f1 / (float)n);
    }

}