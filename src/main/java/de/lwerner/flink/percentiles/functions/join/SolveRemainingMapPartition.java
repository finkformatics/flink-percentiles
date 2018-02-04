package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Function for solving the remaining problem
 *
 * @author Lukas Werner
 */
public class SolveRemainingMapPartition extends RichMapPartitionFunction<Tuple3<Float, Long, Long>, Tuple3<Float, Long, Long>> {

    @Override
    public void mapPartition(Iterable<Tuple3<Float, Long, Long>> values, Collector<Tuple3<Float, Long, Long>> out) {
        List<Tuple3<Float, Long, Long>> valuesList = new ArrayList<>();

        values.forEach(valuesList::add);

        if (valuesList.isEmpty()) {
            throw new IllegalStateException("The remaining elements should never be empty! Please check the code!");
        }

        long k = valuesList.get(0).f1;

        if (valuesList.size() < k) {
            throw new IllegalStateException("The remaining elements are less than k. This should never happen! Please check the code! Remaining size: " + valuesList.size() + ", k: " + k);
        }

        out.collect(valuesList.get((int)k - 1));
    }

}