package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.functions.redis.InputToTupleMapFunction;
import de.lwerner.flink.percentiles.functions.redis.RemainingValuesMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
     * The hdfs default name
     */
    private final String fsDefaultName;

    /**
     * The resulting data set (cached for multiple accesses)
     */
    private DataSet<Tuple1<Float>> dataSet;

    /**
     * The value count (cached for multiple accesses)
     */
    private long count;

    /**
     * Constructor, sets env and path
     *
     * @param env the flink env
     * @param path the hdfs path
     * @param count number of values
     * @param fsDefaultName hdfs default name
     */
    public HdfsSource(ExecutionEnvironment env, String path, long count, String fsDefaultName) {
        this.env = env;
        this.path = path;
        this.count = count;
        this.fsDefaultName = fsDefaultName;
    }

    /**
     * Constructor
     *
     * @param env the flink env
     * @param path the hdfs path
     * @param count the number of values
     */
    public HdfsSource(ExecutionEnvironment env, String path, long count) {
        this(env, path, count, null);
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public DataSet<Tuple1<Float>> getDataSet() {
        if (dataSet == null) {
            dataSet = env.readFileOfPrimitives(path, Float.class)
                    .map(new InputToTupleMapFunction());
        }

        return dataSet;
    }

    @Override
    public List<Float> getValues() throws Exception {
        return getDataSet().map(new RemainingValuesMapFunction()).collect();
    }

    /**
     * Gets the values to work with while you can define if it should work also without flink
     *
     * @param withoutFlink should it work without flink?
     *
     * @return the values
     *
     * @throws Exception if something went wrong
     */
    public List<Float> getValues(boolean withoutFlink) throws Exception {
        if (withoutFlink) {
            URI uri = URI.create(path);
            Path path = new Path(uri);

            Configuration conf = new Configuration();
            conf.set("fs.default.name", fsDefaultName);

            FileSystem dfs = FileSystem.get(uri, conf);

            FSDataInputStream in = dfs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));

            List<Float> result = new ArrayList<>((int)getCount());

            String line;
            while ((line = reader.readLine()) != null) {
                result.add(Float.parseFloat(line));
            }

            reader.close();
            return result;
        }

        return getValues();
    }

    @Override
    public ExecutionEnvironment getEnv() {
        return env;
    }

}