package de.lwerner.flink.percentiles.data;

import de.lwerner.flink.percentiles.ResultReport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

/**
 * Class HdfsSink
 *
 * Defines a sink for writing the result as a text file to hadoop distributed file system
 *
 * @author Lukas Werner
 */
public class HdfsSink implements SinkInterface {

    /**
     * File system default name
     */
    private final String fileSystemDefaultName;
    /**
     * Result file path on hdfs
     */
    private final String path;
    /**
     * Full report wanted?
     */
    private boolean full;

    /**
     * Constructor, sets env and path
     *
     * @param fileSystemDefaultName fs default name
     * @param path the hdfs path
     * @param full full report?
     */
    public HdfsSink(String fileSystemDefaultName, String path, boolean full) {
        this.fileSystemDefaultName = fileSystemDefaultName;
        this.path = path;
        this.full = full;
    }

    @Override
    public void processResult(ResultReport resultReport) throws IOException {
        URI uri = URI.create(path);
        Path path = new Path(uri);

        Configuration conf = new Configuration();
        conf.set("fs.default.name", fileSystemDefaultName);

        FileSystem dfs = FileSystem.get(uri, conf);

        FSDataOutputStream out = dfs.create(path);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));

        if (full) {
            writer.write(resultReport.toString());
        } else {
            writer.write("" + resultReport.getResults()[0]);
        }

        writer.close();
    }

}