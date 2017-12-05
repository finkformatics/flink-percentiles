package de.lwerner.flink.percentiles;

import de.lwerner.flink.percentiles.generation.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This is a generator application to generate test files for performance tests
 *
 * @author Lukas Werner
 */
public class TestFileGenerator implements FlushListener {

    /**
     * The default flush count
     */
    private static final int DEFAULT_FLUSH_COUNT = 1000000;

    /**
     * The actual generator
     */
    private final AbstractGenerator generator;
    /**
     * The file to generate to
     */
    private final File file;

    /**
     * The writer
     */
    private BufferedWriter writer;

    /**
     * Constructor sets generator and file.
     *
     * @param generator the generator to use
     * @param file the file to write to
     */
    public TestFileGenerator(AbstractGenerator generator, File file) {
        this.generator = generator;
        this.file = file;

        this.generator.addFlushListener(this);
    }

    @Override
    public void onFlush(FlushEvent event) {
        for (float val: event.getValues()) {
            try {
                writer.write("" + val);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Actually generate the values
     *
     * @param count the number of values to generate
     */
    public void generate(long count) {
        try {
            writer = new BufferedWriter(new FileWriter(file));
            generator.generate(count);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Main method to start generation
     *
     * @param args program arguments
     *
     * @throws IllegalArgumentException wrong arguments given
     */
    public static void main(String[] args) throws IllegalArgumentException {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: java " + TestFileGenerator.class.getCanonicalName() + " <random|exponential|allequal|asc|desc> <saveToPath> <valueCount>");
        }

        AbstractGenerator generator;
        switch (args[0]) {
            case "random":
                generator = new RandomGenerator(DEFAULT_FLUSH_COUNT);
                break;
            case "exponential":
                generator = new ExponentialGenerator(DEFAULT_FLUSH_COUNT);
                break;
            case "allequal":
                generator = new AllEqualGenerator(DEFAULT_FLUSH_COUNT);
                break;
            case "asc":
                generator = new SortedAscGenerator(DEFAULT_FLUSH_COUNT);
                break;
            case "desc":
                generator = new SortedDescGenerator(DEFAULT_FLUSH_COUNT);
                break;
            default:
                throw new IllegalArgumentException("Please provide the generator as first argument (random|exponential|allequal|asc|desc).");
        }

        File file = new File(args[1]);
        try {
            if (!file.createNewFile()) {
                throw new IllegalArgumentException("Won't override files!");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't write parent on directory!");
        }

        TestFileGenerator testFileGenerator = new TestFileGenerator(generator, file);
        testFileGenerator.generate(Long.valueOf(args[2]));
    }
}