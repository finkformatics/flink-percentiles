package de.lwerner.flink.percentiles.generation;

/**
 * Concrete ascending sorted value generator.
 *
 * @author Lukas Werner
 */
public class SortedAscGenerator extends AbstractGenerator {

    /**
     * The iterator
     */
    private long iterator;

    /**
     * Constructor to call parent constructor and initialize the iterator with 1.
     *
     * @param flushCount the flush count
     */
    public SortedAscGenerator(int flushCount) {
        super(flushCount);

        iterator = 1;
    }

    @Override
    public float generateValue() {
        return (iterator++ * 0.00001f);
    }

}