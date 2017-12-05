package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.model.DecisionModel;
import de.lwerner.flink.percentiles.model.RedisCredentials;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Function for filtering out the decision base, if we have found a solution or we have less or equal elements as
 * the given threshold.
 *
 * @author Lukas Werner
 */
public class TerminationCriterionFilterFunction extends RichFilterFunction<DecisionModel> {

    /**
     * Redis adapter for accessing redis values
     */
    private AbstractRedisAdapter redisAdapter;

    /**
     * The given threshold
     */
    private long threshold;

    /**
     * Redis connection info
     */
    private RedisCredentials redisCredentials;

    /**
     * Constructor to set the redis credentials
     *
     * @param redisCredentials the redis credentials
     */
    public TerminationCriterionFilterFunction(RedisCredentials redisCredentials) {
        this.redisCredentials = redisCredentials;
    }

    @Override
    public void open(Configuration parameters) {
        redisAdapter = AbstractRedisAdapter.factory(redisCredentials);
        threshold = redisAdapter.getT();
    }

    @Override
    public void close() {
        redisAdapter.close();
    }

    @Override
    public boolean filter(DecisionModel decisionModel) {
        long k = decisionModel.getK();
        long n = decisionModel.getN();

        redisAdapter.setNthK(k, 1);
        redisAdapter.setN(n);
        redisAdapter.setNumberOfIterations(getIterationRuntimeContext().getSuperstepNumber());

        return !decisionModel.isFoundResult() && n > threshold;
    }
}