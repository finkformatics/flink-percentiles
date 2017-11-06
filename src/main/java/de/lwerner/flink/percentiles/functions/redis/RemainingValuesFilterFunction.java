package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import de.lwerner.flink.percentiles.util.JedisHelper;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

/**
 * Helper function for discarding all the values, if we already found a solution
 *
 * @author Lukas Werner
 */
public class RemainingValuesFilterFunction extends RichFilterFunction<Float> {

    /**
     * Redis value for indicating if we already found a solution
     */
    private boolean resultFound;

    @Override
    public void open(Configuration parameters) throws Exception {
        AbstractRedisAdapter redisAdapter = AbstractRedisAdapter.factory(AppProperties.getInstance());
        resultFound = redisAdapter.getResultFound();
        redisAdapter.close();
    }

    @Override
    public boolean filter(Float value) throws Exception {
        return !resultFound;
    }
}
