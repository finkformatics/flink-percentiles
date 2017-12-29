package de.lwerner.flink.percentiles.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Function which calculates the weighted median by given medians and weights.
 *
 * @author Lukas Werner
 */
public class CalculateWeightedMedianGroupReduceFunction implements GroupReduceFunction<Tuple2<Float, Float>, Tuple1<Float>> {

    @Override
    public void reduce(Iterable<Tuple2<Float, Float>> iterable, Collector<Tuple1<Float>> collector) {
        List<Float> medians = new ArrayList<>();
        List<Float> weights = new ArrayList<>();
        for (Tuple2<Float, Float> t: iterable) {
            medians.add(t.f0);
            weights.add(t.f1);
        }

        for (float mk: medians) {
            float sumLower = 0;
            float sumHigher = 0;
            for (int j = 0; j < medians.size(); j++) {
                float mi = medians.get(j);
                float wi = weights.get(j);

                if (mi < mk) {
                    sumLower += wi;
                } else if (mi > mk) {
                    sumHigher += wi;
                }
            }

            if (sumLower <= 0.5 && sumHigher <= 0.5) {
                collector.collect(new Tuple1<>(mk));
                break;
            }
        }
    }

}