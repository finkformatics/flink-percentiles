package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.Result;
import de.lwerner.flink.percentiles.timeMeasurement.Timer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Class PrintSink
 *
 * Defines a simple sink for just printing out the result
 *
 * @author Lukas Werner
 */
public class PrintSink extends AbstractSink {

    public PrintSink() {
        super();
    }

    @Override
    public void processResult(Result result) throws Exception {
        if (result.getSolution() != null) {
            DataSet<Tuple4<Long, Integer, Long, Float>> resultInformation = solutionDataSetToTuple(result);
            // Here we just print the result
            resultInformation.print();
        } else {
            System.out.println(result);
        }

        if (result.getTimerResults() != null && result.getTimerResults().containsKey(Timer.DEFAULT_TIMER_NAME)) {
            logger.info("Execution time: {} milliseconds!", result.getTimerResults().get(Timer.DEFAULT_TIMER_NAME));
        }
    }

}