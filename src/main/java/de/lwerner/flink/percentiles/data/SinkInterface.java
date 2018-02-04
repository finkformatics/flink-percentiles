package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;

/**
 * Interface SinkInterface
 *
 * Simple interface to provide methods, required for a data sink in this application
 *
 * @author Lukas Werner
 */
public interface SinkInterface {

    /**
     * Processes the result
     *
     * @param result the result model
     *
     * @throws Exception sink can through an exception
     */
    void processResult(Result result) throws Exception;

}