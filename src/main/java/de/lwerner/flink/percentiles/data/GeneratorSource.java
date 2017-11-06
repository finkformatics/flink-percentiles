package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.functions.redis.InputToTupleMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Class GeneratorSource
 *
 * Defines a data source which generates random values
 *
 * @author Lukas Werner
 */
public class GeneratorSource implements SourceInterface {

    /**
     * The flink execution environment
     */
    private final ExecutionEnvironment env;
    /**
     * Number of elements to generate
     */
    private final long n;

    /**
     * Constructor, sets env and n
     *
     * @param env the flink env
     * @param n the value count
     */
    public GeneratorSource(ExecutionEnvironment env, long n) {
        this.env = env;
        this.n = n;
    }

    @Override
    public long getCount() {
        return n;
    }

    @Override
    public DataSet<Tuple1<Float>> getDataSet() {
        List<Float> values = new ArrayList<>();
        Random rnd = new Random();
        for (long i = 0; i < n; i++) {
            values.add(rnd.nextFloat());
        }

        return env.fromCollection(values)
                .map(new InputToTupleMapFunction());
    }
    
}