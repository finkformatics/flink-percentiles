package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.model.RedisCredentials;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Function which maps each double value to a tuple (x, y, z) which says, if the element is less (1, 0, 0), equal
 * (0, 1, 0) or greater (0, 0, 1) than the weighted median.
 *
 * @author Lukas Werner
 */
public class CalculateLessEqualAndGreaterMapFunction extends RichMapFunction<Tuple1<Float>, Tuple3<Long, Long, Long>> {

    /**
     * The weighted median
     */
    private float weightedMedian;

    /**
     * Redis connection info
     */
    private RedisCredentials redisCredentials;

    /**
     * Constructor to set the redis credentials
     *
     * @param redisCredentials the redis credentials
     */
    public CalculateLessEqualAndGreaterMapFunction(RedisCredentials redisCredentials) {
        this.redisCredentials = redisCredentials;
    }

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple1<Float>> weightedMedian = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Float> t: weightedMedian) {
            this.weightedMedian = t.f0;
        }

        AbstractRedisAdapter redisAdapter = AbstractRedisAdapter.factory(redisCredentials);
        redisAdapter.setResult(this.weightedMedian);
        redisAdapter.close();
    }

    @Override
    public Tuple3<Long, Long, Long> map(Tuple1<Float> t) {
        return new Tuple3<>(t.f0 < weightedMedian ? 1L : 0L, t.f0 == weightedMedian ? 1L : 0L, t.f0 > weightedMedian ? 1L : 0L);
    }

}