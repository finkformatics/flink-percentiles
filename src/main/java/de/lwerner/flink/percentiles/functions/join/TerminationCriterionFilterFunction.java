package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function for filtering out the decision base, if we have found a solution or we have less or equal elements as
 * the given threshold.
 *
 * @author Lukas Werner
 */
public class TerminationCriterionFilterFunction extends RichFilterFunction<Tuple3<Boolean, Long, Long>> {

    private long countThreshold;

    /**
     * Constructor to set count threshold
     *
     * @param countThreshold the count threshold to set
     */
    public TerminationCriterionFilterFunction(long countThreshold) {
        this.countThreshold = countThreshold;
    }

    @Override
    public boolean filter(Tuple3<Boolean, Long, Long> t) {
        return t.f2 > countThreshold;
    }

}