package de.lwerner.flink.percentiles.math;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * A few unit tests for the quick select algorithm
 *
 * @author Lukas Werner
 */
public class QuickSelectTest {

    /**
     * A consistent factor for calculating the correct values
     */
    private static final float FACTOR = 0.0625f;
    /**
     * Maximum allowed error
     */
    private static final float ALLOWED_ERROR = 0.0078125f;

    /**
     * The unit test for checking if the iterative select method works properly
     */
    @Test
    public void select() {
        checkSelectGeneralized(false);
    }

    /**
     * The unit test for checking if the recursive select method works properly
     */
    @Test
    public void selectRecursive() {
        checkSelectGeneralized(true);
    }

    private void checkSelectGeneralized(boolean recursive) {
        QuickSelect quickSelect = new QuickSelect();

        List<Float> values = new LinkedList<>();
        for (int i = 1; i <= 100; i++) {
            values.add(i * FACTOR);
        }

        Collections.shuffle(values);
        Collections.shuffle(values);
        Collections.shuffle(values);

        // Please note, that in general k is ordinary, which means counts from 1 on, so for array indexes it's minus 1
        float result01 = quickSelect.select(values, 0, recursive);
        float result25 = quickSelect.select(values, 24, recursive);
        float result50 = quickSelect.select(values, 49, recursive);
        float result75 = quickSelect.select(values, 74, recursive);
        float result99 = quickSelect.select(values, 98, recursive);

        assertEquals(1 * FACTOR, result01, ALLOWED_ERROR);
        assertEquals(25 * FACTOR, result25, ALLOWED_ERROR);
        assertEquals(50 * FACTOR, result50, ALLOWED_ERROR);
        assertEquals(75 * FACTOR, result75, ALLOWED_ERROR);
        assertEquals(99 * FACTOR, result99, ALLOWED_ERROR);
    }

}