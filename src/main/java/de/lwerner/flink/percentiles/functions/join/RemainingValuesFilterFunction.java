package de.lwerner.flink.percentiles.functions.join;

import de.lwerner.flink.percentiles.util.JedisHelper;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

/**
 * Helper function for discarding all the values, if we already found a solution
 *
 * @author Lukas Werner
 */
public class RemainingValuesFilterFunction extends RichFilterFunction<Double> {

    /**
     * Redis value for indicating if we already found a solution
     */
    private boolean resultFound;

    @Override
    public boolean filter(Double value) throws Exception {
        return !resultFound;
    }
}
