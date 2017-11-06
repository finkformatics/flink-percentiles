package de.lwerner.flink.percentiles.functions.redis;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Function which calculates the median and value count for each partition, given to it
 *
 * @author Lukas Werner
 */
public class MedianAndCountMapPartitionFunction implements MapPartitionFunction<Tuple1<Float>, Tuple2<Float, Long>> {

    @Override
    public void mapPartition(Iterable<Tuple1<Float>> iterable, Collector<Tuple2<Float, Long>> collector) throws Exception {
        List<Float> list = new ArrayList<>();
        for (Tuple1<Float> t: iterable) {
            list.add(t.f0);
        }

        float median;
        if (list.size() % 2 == 0) {
            median = (list.get(list.size() / 2) + list.get(list.size() / 2 - 1)) / 2;
        } else {
            median = list.get(list.size() / 2);
        }

        collector.collect(new Tuple2<>(median, (long)list.size()));
    }

}