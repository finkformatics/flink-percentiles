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

                String fsDefaultName = params.getRequired("fs-default-name");
                if (!fsDefaultName.startsWith("hdfs://")) {
                    throw new IllegalArgumentException("FS default name must start with hdfs://");
                }

                sink = new HdfsSink(fsDefaultName, path);
                break;
            default:
                throw new IllegalArgumentException("You must provide an processResult: --processResult <print|hdfs>");
        }

        return sink;
    }

}