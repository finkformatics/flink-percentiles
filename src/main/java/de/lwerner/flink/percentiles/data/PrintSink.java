package de.lwerner.flink.percentiles.data;

/**
 * Class PrintSink
 *
 * Defines a simple sink for just printing out the result
 *
 * @author Lukas Werner
 */
public class PrintSink implements SinkInterface {

    @Override
    public void processResult(float result) {
        System.out.println(result);
    }

}