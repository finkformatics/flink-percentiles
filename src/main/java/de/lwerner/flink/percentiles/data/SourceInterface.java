package de.lwerner.flink.percentiles.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.List;

/**
 * Interface SourceInterface
 *
 * Simple interface to provide methods, required for a data source in this application
 *
 * @author Lukas Werner
 */
public interface SourceInterface {

    /**
     * Returns a value count for this source
     *
     * @return the value count
     */
    long getCount();

    /**
     * Returns the data set in the proper format
     *
     * @return float values data set
     *
     * @throws Exception if something goes wrong
     */
    DataSet<Tuple1<Float>> getDataSet() throws Exception;

    /**
     * Get the values as list
     *
     * @return value list
     *
     * @throws Exception if something goes wrong
     */
    List<Float> getValues() throws Exception;

    /**
     * Get the execution environment
     *
     * @return the execution environment
     */
    ExecutionEnvironment getEnv();

}