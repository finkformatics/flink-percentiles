package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.functions.redis.InputToTupleMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.ArrayList;
import java.util.List;

/**
 * Class AllEqualGeneratorSource
 *
 * Defines a data source which generates all equal values
 *
 * @author Lukas Werner
 */
public class AllEqualGeneratorSource implements SourceInterface {

    /**
     * The flink execution environment
     */
    private final ExecutionEnvironment env;
    /**
     * Number of elements to generate
     */
    private final long n;
    /**
     * The cached values
     */
    private List<Float> values;

    /**
     * Constructor, sets env and n
     *
     * @param env the flink env
     * @param n the value count
     */
    public AllEqualGeneratorSource(ExecutionEnvironment env, long n) {
        this.env = env;
        this.n = n;
    }

    @Override
    public long getCount() {
        return n;
    }

    @Override
    public DataSet<Tuple1<Float>> getDataSet() throws Exception {
        return env.fromCollection(getValues())
                .map(new InputToTupleMapFunction());
    }

    @Override
    public List<Float> getValues() throws Exception {
        if (values == null) {
            values = new ArrayList<>();
            for (long i = 0; i < n; i++) {
                values.add(1f);
            }
        }

        return values;
    }

    @Override
    public ExecutionEnvironment getEnv() {
        return env;
    }

}