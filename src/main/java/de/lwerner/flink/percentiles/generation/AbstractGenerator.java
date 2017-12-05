package de.lwerner.flink.percentiles.generation;

import java.util.ArrayList;

/**
 * Abstract generator class. Provides functionality to generate values by given concrete generator.
 *
 * @author Lukas Werner
 */
public abstract class AbstractGenerator {

    /**
     * The flush count, each (flushCount)th value the flush event will be fired
     */
    private final int flushCount;

    /**
     * Flush listeners to react on flush events
     */
    private final ArrayList<FlushListener> flushListeners;

    /**
     * The constructor to initialize all required stuff.
     *
     * @param flushCount the flush count
     */
    public AbstractGenerator(int flushCount) {
        this.flushCount = flushCount;

        flushListeners = new ArrayList<>();
    }

    /**
     * Add a flush listener which reacts on flush events
     *
     * @param flushListener the flush listener to add
     */
    public void addFlushListener(FlushListener flushListener) {
        flushListeners.add(flushListener);
    }

    /**
     * Remove a flush listener
     *
     * @param flushListener the flush listener to remove
     */
    public void removeFlushListener(FlushListener flushListener) {
        flushListeners.remove(flushListener);
    }

    /**
     * Main generate method, generates values and periodically fires flush events after all (flushCount) values
     *
     * @param count the total value count to generate
     */
    public void generate(long count) {
        ArrayList<Float> tempValues = new ArrayList<>(flushCount);
        for (long i = 0; i < count; i++) {
            tempValues.add(generateValue());

            if ((i + 1) % flushCount == 0) {
                flush(tempValues);
                tempValues.clear();
            }
        }
    }

    /**
     * Internal flush method: Creates flush event and fires the listener methods
     *
     * @param values the values generated since start or last flush
     */
    private void flush(ArrayList<Float> values) {
        ArrayList<Float> newValues = new ArrayList<>(values);
        FlushEvent flushEvent = new FlushEvent(newValues);
        for (FlushListener flushListener: flushListeners) {
            flushListener.onFlush(flushEvent);
        }
    }

    /**
     * Method to generate a single value. Is implemented by concrete generator classes.
     *
     * @return the generated value
     */
    public abstract float generateValue();

}