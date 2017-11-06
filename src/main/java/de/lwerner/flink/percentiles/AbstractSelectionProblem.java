package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.data.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;

public abstract class AbstractSelectionProblem {

    /**
     * The data source for delivering data to application
     */
    private SourceInterface source;

    /**
     * The data sink for putting out the result
     */
    private SinkInterface sink;

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
        this.source = source;
        this.k = k;
        this.t = t;
        this.sink = sink;
    }

    /**
     * Get the source
     *
     * @return the source
     */
    public SourceInterface getSource() {
        return source;
    }

    /**
     * Get the sink
     *
     * @return the sink
     */
    public SinkInterface getSink() {
        return sink;
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

        String dataSource = params.get("source", "generator");

        SourceInterface source;
        long n;
        switch (dataSource) {
            case "generator":
                n = Long.valueOf(params.getRequired("count"));
                if (n == 0) {
                    throw new IllegalArgumentException("If you're using the generator data source, please provide a value count: --count <num>");
                }

                source = new GeneratorSource(env, n);
                break;
            case "exponential":
                n = Long.valueOf(params.getRequired("count"));
                if (n == 0) {
                    throw new IllegalArgumentException("If you're using the exponential data source, please provide a value count: --count <num>");
                }

                source = new ExponentialSource(env, n);
                break;
            case "hdfs":
                String path = params.getRequired("input-path");
                if (!path.startsWith("hdfs://")) {
                    throw new IllegalArgumentException("Path must start with hdfs://");
                }

                source = new HdfsSource(env, path);
                break;
            default:
                throw new IllegalArgumentException("You must provide a source: --source <generator|hdfs>");
        }

        String output = params.get("output", "print");

        SinkInterface sink;
        switch (output) {
            case "print":
                sink = new PrintSink();
                break;
            case "hdfs":
                String path = params.getRequired("output-path");
                if (!path.startsWith("hdfs://")) {
                    throw new IllegalArgumentException("Path must start with hdfs://");
                }

                String fsDefaultName = params.getRequired("fs-default-name");
                if (!path.startsWith("hdfs://")) {
                    throw new IllegalArgumentException("FS default name must start with hdfs://");
                }

                sink = new HdfsSink(fsDefaultName, path);
                break;
            default:
                throw new IllegalArgumentException("You must provide an processResult: --processResult <print|hdfs>");
        }

        for (long kVal: k) {
            if (kVal < 1 || kVal > source.getCount()) {
                throw new IllegalArgumentException("k must be between 1 and the value count");
            }
        }

        long t = Long.valueOf(params.get("serial-threshold", "1000"));

        if (t < 100) {
            throw new IllegalArgumentException("Please provide a serial threshold of at least 100");
        }

        return clazz.getDeclaredConstructor(SourceInterface.class, SinkInterface.class, long[].class, long.class)
                .newInstance(source, sink, k, t);
    }

    /**
     * Solves the algorithm, using the field source as data source and the field sink as data sink.
     *
     * @throws Exception if anything goes wrong
     */
    public abstract void solve() throws Exception;

}