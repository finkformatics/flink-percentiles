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

    public static SourceInterface getSourceFromParams(ParameterTool params, ExecutionEnvironment env, long n) {
        SourceInterface source;

        String dataSource = params.get("source", "generator");

        switch (dataSource) {
            case "generator":
                source = new GeneratorSource(env, n);
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

    public static SinkInterface getSinkFromParams(ParameterTool params) {
        SinkInterface sink;

        String dataSink = params.get("sink", "print");

        String fullStr = params.get("full", "false");
        boolean full = fullStr.equals("true");

        switch (dataSink) {
            case "print":
                sink = new PrintSink(full);
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

                sink = new HdfsSink(fsDefaultName, path, full);
                break;
            default:
                throw new IllegalArgumentException("You must provide an processResult: --processResult <print|hdfs>");
        }

        return sink;
    }

}