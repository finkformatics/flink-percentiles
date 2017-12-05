package de.lwerner.flink.percentiles.generation;

/**
 * Concrete equal value generator. Generates only 1.0 values.
 *
 * @author Lukas Werner
 */
public class AllEqualGenerator extends AbstractGenerator {

    /**
     * Constructor to call parent constructor.
     *
     * @param flushCount the flush count
     */
    public AllEqualGenerator(int flushCount) {
        super(flushCount);
    }

    @Override
    public float generateValue() {
        return 1F;
    }

}