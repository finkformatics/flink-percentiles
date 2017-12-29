package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Function which calculates the median and value count for each partition, given to it
 *
 * @author Lukas Werner
 */
public class MedianAndCountMapPartitionFunction implements MapPartitionFunction<Tuple3<Float, Long, Long>, Tuple3<Float, Long, Long>> {

    @Override
    public void mapPartition(Iterable<Tuple3<Float, Long, Long>> iterable, Collector<Tuple3<Float, Long, Long>> collector) {
        List<Float> list = new ArrayList<>();
        long n = 0;
        for (Tuple3<Float, Long, Long> t: iterable) {
            list.add(t.f0);
            n = t.f2;
        }

        float median;
        if (list.size() % 2 == 0) {
            median = (list.get(list.size() / 2) + list.get(list.size() / 2 - 1)) / 2;
        } else {
            median = list.get(list.size() / 2);
        }

        collector.collect(new Tuple3<>(median, (long)list.size(), n));
    }

}