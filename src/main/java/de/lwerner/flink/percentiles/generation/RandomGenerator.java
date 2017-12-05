package de.lwerner.flink.percentiles.generation;

import java.util.Random;

/**
 * Concrete random value generator. Generates pseudo random values.
 *
 * @author Lukas Werner
 */
public class RandomGenerator extends AbstractGenerator {

    /**
     * The java random object
     */
    private final Random random;

    /**
     * Constructor to call parent constructor and to initialize random object.
     *
     * @param flushCount the flush count
     */
    public RandomGenerator(int flushCount) {
        super(flushCount);

        random = new Random(System.currentTimeMillis());
    }

    @Override
    public float generateValue() {
        return random.nextFloat();
    }

}