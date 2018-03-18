package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Class HdfsSink
 * <p>
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
        super();

        this.path = path;
    }

    @Override
    public void processResult(Result result) throws Exception {
        JobExecutionResult jobExecutionResult = null;
        if (result.getSolution() != null) {
            DataSet<Tuple4<Long, Integer, Long, Float>> resultInformation = solutionDataSetToTuple(result);
            resultInformation.writeAsCsv(path, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
            jobExecutionResult = resultInformation.getExecutionEnvironment().execute();
        }

        if (jobExecutionResult != null) {
            logger.info("Execution time: {} milliseconds!", jobExecutionResult.getNetRuntime());
        }
    }

}