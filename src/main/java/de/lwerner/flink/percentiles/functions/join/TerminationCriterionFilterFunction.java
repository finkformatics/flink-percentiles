package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Function for filtering out the decision base, if we have found a solution or we have less or equal elements as
 * the given threshold.
 *
 * @author Lukas Werner
 */
public class TerminationCriterionFilterFunction extends RichFilterFunction<Tuple5<Boolean, Boolean, Float, Long, Long>> {

    private long countThreshold;

    public TerminationCriterionFilterFunction(long countThreshold) {
        this.countThreshold = countThreshold;
    }

    @Override
    public boolean filter(Tuple5<Boolean, Boolean, Float, Long, Long> t) {
        return !t.f0 && t.f4 > countThreshold;
    }

}