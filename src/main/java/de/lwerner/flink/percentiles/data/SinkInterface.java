package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.ResultReport;

import java.io.IOException;

/**
 * Interface SinkInterface
 *
 * Simple interface to provide methods, required for a data sink in this application
 *
 * @author Lukas Werner
 */
public interface SinkInterface {

    /**
     * Processes a result report
     *
     * @param resultReport the result report
     *
     * @throws IOException sink can be an IO operation, can through an exception though
     */
    void processResult(ResultReport resultReport) throws IOException;

}