package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Class HdfsSink
 * <p>
 * Defines a sink for writing the result as a text file to hadoop distributed file system
 *
 * @author Lukas Werner
 */
public class HdfsSink extends AbstractSink {

    /**
     * The hdfs default name
     */
    private final String fileSystemDefaultName;

    /**
     * Result file path on hdfs
     */
    private final String path;

    /**
     * Constructor, sets env and path
     *
     * @param path the hdfs path
     */
    public HdfsSink(String fileSystemDefaultName, String path) {
        super();

        this.fileSystemDefaultName = fileSystemDefaultName;
        this.path = path;
    }

    @Override
    public void processResult(Result result) throws Exception {
        JobExecutionResult jobExecutionResult = null;
        if (result.getSolution() != null) {
            DataSet<Tuple4<Long, Integer, Long, Float>> resultInformation = solutionDataSetToTuple(result);
            resultInformation.writeAsCsv(path, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
            jobExecutionResult = resultInformation.getExecutionEnvironment().execute();
        } else {
            URI uri = URI.create(path);
            Path path = new Path(uri);

            Configuration conf = new Configuration();
            conf.set("fs.default.name", fileSystemDefaultName);

            FileSystem dfs = FileSystem.get(uri, conf);

            FSDataOutputStream out = dfs.create(path);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));

            writer.write(result.toString());
            writer.close();
        }

        if (jobExecutionResult != null) {
            logger.info("Execution time: {} milliseconds!", jobExecutionResult.getNetRuntime());
        }
    }

}