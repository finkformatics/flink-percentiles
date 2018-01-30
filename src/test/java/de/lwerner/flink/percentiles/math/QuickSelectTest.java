package de.lwerner.flink.percentiles.math;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class QuickSelectTest {

    private static final float FACTOR = 0.0625f;
    private static final float ALLOWED_ERROR = 0.0078125f;

    @Test
    public void select() {
        QuickSelect quickSelect = new QuickSelect();

        List<Float> values = new LinkedList<>();
        for (int i = 1; i <= 100; i++) {
            values.add(i * FACTOR);
        }

        Collections.shuffle(values);
        Collections.shuffle(values);
        Collections.shuffle(values);

        float result01 = quickSelect.select(values, 1 - 1);
        float result25 = quickSelect.select(values, 25 - 1);
        float result50 = quickSelect.select(values, 50 - 1);
        float result75 = quickSelect.select(values, 75 - 1);
        float result99 = quickSelect.select(values, 99 - 1);

        assertEquals(1 * FACTOR, result01, ALLOWED_ERROR);
        assertEquals(25 * FACTOR, result25, ALLOWED_ERROR);
        assertEquals(50 * FACTOR, result50, ALLOWED_ERROR);
        assertEquals(75 * FACTOR, result75, ALLOWED_ERROR);
        assertEquals(99 * FACTOR, result99, ALLOWED_ERROR);
    }

    @Test
    public void selectRecursive() {
        QuickSelect quickSelect = new QuickSelect();

        List<Float> values = new LinkedList<>();
        for (int i = 1; i <= 100; i++) {
            values.add(i * FACTOR);
        }

        Collections.shuffle(values);
        Collections.shuffle(values);
        Collections.shuffle(values);

        float result01 = quickSelect.select(values, 1 - 1, true);
        float result25 = quickSelect.select(values, 25 - 1, true);
        float result50 = quickSelect.select(values, 50 - 1, true);
        float result75 = quickSelect.select(values, 75 - 1, true);
        float result99 = quickSelect.select(values, 99 - 1, true);

        assertEquals(1 * FACTOR, result01, ALLOWED_ERROR);
        assertEquals(25 * FACTOR, result25, ALLOWED_ERROR);
        assertEquals(50 * FACTOR, result50, ALLOWED_ERROR);
        assertEquals(75 * FACTOR, result75, ALLOWED_ERROR);
        assertEquals(99 * FACTOR, result99, ALLOWED_ERROR);
    }

}