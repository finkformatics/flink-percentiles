package de.lwerner.flink.percentiles.generation;

/**
 * Flush listener to implement on classes which use the generators
 *
 * @author Lukas Werner
 */
public interface FlushListener {

    /**
     * Fired, when a flush event is created
     *
     * @param event the flush event
     */
    void onFlush(FlushEvent event);

}