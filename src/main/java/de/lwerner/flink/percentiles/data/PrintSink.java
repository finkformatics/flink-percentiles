package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;

/**
 * Class PrintSink
 *
 * Defines a simple sink for just printing out the result
 *
 * @author Lukas Werner
 */
public class PrintSink implements SinkInterface {

    @Override
    public void processResult(Result result) throws Exception {
        // Here we just print the result
        result.getSolution().print();
    }

}