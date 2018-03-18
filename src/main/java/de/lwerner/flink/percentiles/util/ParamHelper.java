package de.lwerner.flink.percentiles.util;

import de.lwerner.flink.percentiles.data.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Class ParamHelper provides methods for simplifying param handling.
 *
 * @author Lukas Werner
 */
public class ParamHelper {

    /**
     * Set parallelism to execution environment defined by params, default is 8
     *
     * @param params the parameter tool from flink api
     * @param env the execution environment from flink
     */
    public static void extractParallelismFromParams(ParameterTool params, ExecutionEnvironment env) {
        String parallelismString = params.get("parallelism", "8");
        int parallelism = Integer.valueOf(parallelismString);

        if (parallelism < 1) {
            throw new IllegalArgumentException("Please provide parallelism greater than 0!");
        }

        env.setParallelism(parallelism);
    }

    /**
     * Get the data source from parameters
     *
     * @param params the flink parameter tool
     * @param env    the flink execution environment
     * @param n      number of values to read
     *
     * @return the concrete data source
     */
    public static SourceInterface getSourceFromParams(ParameterTool params, ExecutionEnvironment env, long n) {
        SourceInterface source;

        String dataSource = params.get("source", "random");

        switch (dataSource) {
            case "random":
                source = new RandomSource(env, n);
                break;
            case "exponential":
                source = new ExponentialSource(env, n);
                break;
            case "allequal":
                source = new AllEqualGeneratorSource(env, n);
                break;
            case "asc":
                source = new SortedAscGeneratorSource(env, n);
                break;
            case "desc":
                source = new SortedDescGeneratorSource(env, n);
                break;
            case "hdfs":
                String path = params.getRequired("input-path");
                if (!path.startsWith("hdfs://")) {
                    throw new IllegalArgumentException("Path must start with hdfs://");
                }

                source = new HdfsSource(env, path, n);
                break;
            default:
                throw new IllegalArgumentException("You must provide a source: --source <generator|hdfs>");
        }

        return source;
    }

    /**
     * Get the data sink from parameters
     *
     * @param params the flink parameter tool
     *
     * @return the concrete data sink
     */
    public static SinkInterface getSinkFromParams(ParameterTool params) {
        SinkInterface sink;

        String dataSink = params.get("sink", "print");

        switch (dataSink) {
            case "print":
                sink = new PrintSink();
                break;
            case "hdfs":
                String path = params.getRequired("output-path");
                if (!path.startsWith("hdfs://")) {
                    throw new IllegalArgumentException("Path must start with hdfs://");
                }

                sink = new HdfsSink(path);
                break;
            default:
                throw new IllegalArgumentException("You must provide an processResult: --processResult <print|hdfs>");
        }

        return sink;
    }

}