package de.lwerner.flink.percentiles.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;

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
     */
    DataSet<Tuple1<Float>> getDataSet();

}