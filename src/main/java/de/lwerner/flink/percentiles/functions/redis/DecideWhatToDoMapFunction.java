package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.model.DecisionModel;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * Function, which gets the number of less, equal and greater elements than the weighted median, and decides, what
 * to do. The decisions are:
 *   - Stop, because we found a result
 *   - Discard greater and equal values and set n to the number of less values
 *   - Discard less and equal values and set n to the number of greater values and subtract k by |less| + |equal|
 *
 * @author Lukas Werner
 */
public class DecideWhatToDoMapFunction extends RichMapFunction<Tuple3<Long, Long, Long>, DecisionModel> {

    /**
     * Redis adapter for accessing redis
     */
    private AbstractRedisAdapter redisAdapter;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAdapter = AbstractRedisAdapter.factory(AppProperties.getInstance());
    }

    @Override
    public void close() throws Exception {
        redisAdapter.close();
    }

    @Override
    public DecisionModel map(Tuple3<Long, Long, Long> t) throws Exception {
        long k = redisAdapter.getK();
        long n = redisAdapter.getN();

        boolean foundResult = false;
        boolean keepLess = false;
        float result = 0;

        if (t.f0 < k && k <= t.f0 + t.f1) {
            foundResult = true;
        } else if (k <= t.f0) {
            keepLess = true;
            n = t.f0;
        } else if (k > t.f0 + t.f1) {
            n = t.f2;
            k -= (t.f0 + t.f1);
        }

        redisAdapter.setResultFound(foundResult);

        return new DecisionModel(foundResult, keepLess, result, k, n);
    }

}