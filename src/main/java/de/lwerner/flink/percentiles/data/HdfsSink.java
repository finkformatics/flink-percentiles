package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;

/**
 * Class HdfsSink
 *
 * Defines a sink for writing the result as a text file to hadoop distributed file system
 *
 * @author Lukas Werner
 */
public class HdfsSink extends AbstractSink {

    /**
     * Result file path on hdfs
     */
    private final String path;

    /**
     * Constructor, sets env and path
     *
     * @param path the hdfs path
     */
    public HdfsSink(String path) {
        this.path = path;
    }

    @Override
    public void processResult(Result result) throws Exception {
        DataSet<Tuple4<Long, Integer, Long, Float>> resultInformation = solutionDataSetToTuple(result);
        resultInformation.writeAsCsv(path, FileSystem.WriteMode.OVERWRITE);
        resultInformation.getExecutionEnvironment().execute();

        // TODO: Log execution time with result from execute()
    }

}