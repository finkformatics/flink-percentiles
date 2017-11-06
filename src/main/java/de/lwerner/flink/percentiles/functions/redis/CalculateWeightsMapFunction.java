package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import de.lwerner.flink.percentiles.util.JedisHelper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

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

    @Override
    public void open(Configuration parameters) throws Exception {
        AbstractRedisAdapter redisAdapter = AbstractRedisAdapter.factory(AppProperties.getInstance());
        n = redisAdapter.getN();
        redisAdapter.close();
    }

    @Override
    public Tuple2<Float, Float> map(Tuple2<Float, Long> medianCountAndN) throws Exception {
        return new Tuple2<>(medianCountAndN.f0, medianCountAndN.f1 / (float)n);
    }

}