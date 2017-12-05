package de.lwerner.flink.percentiles.generation;

/**
 * Concrete ascending sorted value generator.
 *
 * @author Lukas Werner
 */
public class SortedDescGenerator extends AbstractGenerator {

    /**
     * The iterator
     */
    private long iterator;

    /**
     * Constructor to call parent constructor and initialize the iterator with 1.
     *
     * @param flushCount the flush count
     */
    public SortedDescGenerator(int flushCount) {
        super(flushCount);
    }

    @Override
    public float generateValue() {
        return (iterator-- * 0.00001f);
    }

    @Override
    public void generate(long count) {
        this.iterator = count;

        super.generate(count);
    }
}