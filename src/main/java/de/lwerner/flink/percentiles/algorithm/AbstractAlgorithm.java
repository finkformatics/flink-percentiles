package de.lwerner.flink.percentiles.algorithm;

import de.lwerner.flink.percentiles.data.SinkInterface;
import de.lwerner.flink.percentiles.data.SourceInterface;
import de.lwerner.flink.percentiles.timeMeasurement.Timer;

/**
 * Abstract algorithm, implements solvable, defines an algorithm which is to be solved.
 *
 * @author Lukas Werner
 */
public abstract class AbstractAlgorithm implements Solvable {

    /**
     * Data source
     */
    private SourceInterface source;

    /**
     * Data sink
     */
    private SinkInterface sink;

    /**
     * Timer utility
     */
    private Timer timer;

    /**
     * Sets source and sink
     *
     * @param source data source
     * @param sink data sink
     */
    public AbstractAlgorithm(SourceInterface source, SinkInterface sink) {
        this.source = source;
        this.sink = sink;

        timer = new Timer();
    }

    /**
     * Get the data source
     *
     * @return the data source
     */
    public SourceInterface getSource() {
        return source;
    }

    /**
     * Get the data sink
     *
     * @return the data sink
     */
    public SinkInterface getSink() {
        return sink;
    }

    /**
     * Get the timer
     *
     * @return the timer
     */
    public Timer getTimer() {
        return timer;
    }
}