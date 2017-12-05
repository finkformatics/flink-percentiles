package de.lwerner.flink.percentiles.algorithm;

import de.lwerner.flink.percentiles.data.*;
import de.lwerner.flink.percentiles.redis.AbstractRedisAdapter;
import de.lwerner.flink.percentiles.util.AppProperties;
import de.lwerner.flink.percentiles.util.ParamHelper;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;

/**
 * Abstract class for generalizing all the repeated work.
 *
 * @author Lukas Werner
 */
public abstract class AbstractSelectionProblem extends AbstractAlgorithm {

    /**
     * The ranks of the searched numbers
     */
    private long[] k;

    /**
     * A threshold at which we can compute serially
     */
    private long t;

    /**
     * AbstractSelectionProblem constructor, sets the required values
     *
     * @param source the data source
     * @param sink the data sink
     * @param k the ranks
     * @param t serial computation threshold
     */
    protected AbstractSelectionProblem(SourceInterface source, SinkInterface sink, long[] k, long t) {
        super(source, sink);

        this.k = k;
        this.t = t;
    }

    /**
     * Get the k array
     *
     * @return k array
     */
    public long[] getK() {
        return k;
    }

    /**
     * Get the first k value in array
     *
     * @return first k
     */
    public long getFirstK() {
        if (k.length < 1) {
            return -1;
        }

        return k[0];
    }

    /**
     * Get the t value
     *
     * @return t
     */
    public long getT() {
        return t;
    }

    /**
     * Factory method for creating the correct algorithm class
     *
     * @param clazz the class to initiate
     * @param params the param tool
     * @param k the k values
     *
     * @param <T> the type of the class to instantiate
     *
     * @return the algorithm class object - can execute solve()
     *
     * @throws NoSuchMethodException if the constructor wasn't found
     * @throws IllegalAccessException if we cannot access the constructor
     * @throws InvocationTargetException if the target isn't able to be invoked
     * @throws InstantiationException if we couldn't instantiate
     */
    public static <T extends AbstractSelectionProblem> T factory(Class<T> clazz, ParameterTool params, long[] k) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long n = Long.valueOf(params.getRequired("count"));
        if (n == 0) {
            throw new IllegalArgumentException("If you're using the generator data source, please provide a value count: --count <num>");
        }

        SourceInterface source = ParamHelper.getSourceFromParams(params, env, n);
        SinkInterface sink = ParamHelper.getSinkFromParams(params);

        for (long kVal: k) {
            if (kVal < 1 || kVal > source.getCount()) {
                throw new IllegalArgumentException("k must be between 1 and the value count");
            }
        }

        long t = Long.valueOf(params.get("t", "1000"));

        if (t < 100) {
            throw new IllegalArgumentException("Please provide a serial threshold of at least 100");
        }

        String redisPassword = params.get("redis-password");
        if (null != redisPassword) {
            AbstractRedisAdapter.setRedisPassword(redisPassword);
        }

        String propertiesFilePath = params.get("propertiesFilePath");
        if (null != propertiesFilePath) {
            AppProperties.setCustomFilePath(propertiesFilePath);
        }

        return clazz.getDeclaredConstructor(SourceInterface.class, SinkInterface.class, long[].class, long.class)
                .newInstance(source, sink, k, t);
    }

}