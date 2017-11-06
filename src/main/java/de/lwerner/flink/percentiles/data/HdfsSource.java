package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.functions.redis.InputToTupleMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Class HdfsSource
 *
 * Defines a data source which connects to hdfs and reads a specific file for input.
 *
 * @author Lukas Werner
 */
public class HdfsSource implements SourceInterface {

    /**
     * The flink env
     */
    private final ExecutionEnvironment env;
    /**
     * The hdfs path
     */
    private final String path;

    /**
     * The resulting data set (cached for multiple accesses)
     */
    private DataSet<Tuple1<Float>> dataSet;

    /**
     * The value count (cached for multiple accesses)
     */
    private long count = -1;

    /**
     * Constructor, sets env and path
     *
     * @param env the flink env
     * @param path the hdfs path
     */
    public HdfsSource(ExecutionEnvironment env, String path) {
        this.env = env;
        this.path = path;
    }

    @Override
    public long getCount() {
        try {
            if (count == -1) {
                count = getDataSet().count();
            }

            return count;
        } catch (Exception e) {
            return 0L;
        }
    }

    @Override
    public DataSet<Tuple1<Float>> getDataSet() {
        if (dataSet == null) {
            dataSet = env.readFileOfPrimitives(path, Float.class)
                    .map(new InputToTupleMapFunction());
        }

        return dataSet;
    }

}