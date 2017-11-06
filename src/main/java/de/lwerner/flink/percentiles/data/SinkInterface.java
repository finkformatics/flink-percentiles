package de.lwerner.flink.percentiles.data;

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
     * Processes a float result (k-th smallest element of a set of floats)
     *
     * @param result the result
     *
     * @throws IOException sink can be an IO operation, can through an exception though
     */
    void processResult(float result) throws IOException;

}