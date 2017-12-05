package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.model.ResultReport;

/**
 * Class PrintSink
 *
 * Defines a simple sink for just printing out the result
 *
 * @author Lukas Werner
 */
public class PrintSink implements SinkInterface {

    /**
     * Full report? Or just the value?
     */
    private boolean full;

    /**
     * Set value for full report or not
     *
     * @param full true for full report
     */
    public PrintSink(boolean full) {
        this.full = full;
    }

    @Override
    public void processResult(ResultReport resultReport) {
        if (full) {
            System.out.println(resultReport);
        } else {
            System.out.println(resultReport.getResults()[0]);
        }
    }

}