package de.lwerner.flink.percentiles.algorithm;

import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.util.AppProperties;
import de.lwerner.flink.percentiles.util.ParamHelper;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;

/**
 * Abstract percentile algorithm class to generalize repeated work.
 *
 * @author Lukas Werner
 */
public abstract class AbstractPercentile extends AbstractAlgorithm {

    /**
     * Percent value
     */
    private int p;

    /**
     * Threshold value
     */
    private long t;

    /**
     * k
     */
    private int k;

    /**
     * Constructor to set the required values;
     *
     * @param source data source
     * @param sink data sink
     * @param p percentage value
     * @param t threshold value
     */
    protected AbstractPercentile(SourceInterface source, SinkInterface sink, int p, long t) {
        super(source, sink);

        this.p = p;
        this.t = t;
    }

    /**
     * Get the percentage value
     *
     * @return the percentage value
     */
    public int getP() {
        return p;
    }

    /**
     * Get the threshold value
     *
     * @return the threshold value
     */
    public long getT() {
        return t;
    }

    /**
     * Get the k value
     *
     * @return k value
     */
    public int getK() {
        return k;
    }

    /**
     * Set the k value
     *
     * @param k k value
     */
    public void setK(int k) {
        this.k = k;
    }

    /**
     * Factory method for creating the correct algorithm class
     *
     * @param clazz the class to initiate
     * @param params the param tool
     * @param p the p value
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
    public static <T> T factory(Class<T> clazz, ParameterTool params, int p) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long n = Long.valueOf(params.getRequired("count"));
        if (n == 0) {
            throw new IllegalArgumentException("If you're using the generator data source, please provide a value count: --count <num>");
        }

        SourceInterface source = ParamHelper.getSourceFromParams(params, env, n);
        SinkInterface sink = ParamHelper.getSinkFromParams(params);

        ParamHelper.extractParallelismFromParams(params, env);

        long t = Long.valueOf(params.get("t", "1000"));

        if (t < 100) {
            throw new IllegalArgumentException("Please provide a serial threshold of at least 100");
        }

        String propertiesFilePath = params.get("propertiesFilePath");
        if (null != propertiesFilePath) {
            AppProperties.setCustomFilePath(propertiesFilePath);
        }

        return clazz.getDeclaredConstructor(SourceInterface.class, SinkInterface.class, int.class, long.class)
                .newInstance(source, sink, p, t);
    }

}