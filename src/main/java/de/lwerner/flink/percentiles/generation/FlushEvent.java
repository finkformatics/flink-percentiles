package de.lwerner.flink.percentiles.generation;

import java.util.ArrayList;

/**
 * The flush event. Holds the values.
 *
 * @author Lukas Werner
 */
public class FlushEvent {

    /**
     * The values
     */
    private final ArrayList<Float> values;

    /**
     * Constructor sets values.
     *
     * @param values the values to set
     */
    public FlushEvent(ArrayList<Float> values) {
        this.values = values;
    }

    /**
     * Getter for values
     *
     * @return the values
     */
    public ArrayList<Float> getValues() {
        return values;
    }
}