package de.lwerner.flink.percentiles.generation;

import java.util.Random;

/**
 * Concrete exponential random value generator. Generates a discrete exponential distribution.
 *
 * @author Lukas Werner
 */
public class ExponentialGenerator extends AbstractGenerator {

    /**
     * The java random object
     */
    private final Random random;

    /**
     * Constructor to call parent constructor and to initialize random object.
     *
     * @param flushCount the flush count
     */
    public ExponentialGenerator(int flushCount) {
        super(flushCount);

        random = new Random(System.currentTimeMillis());
    }

    @Override
    public float generateValue() {
        return -(float)Math.log(1 - random.nextFloat());
    }

}