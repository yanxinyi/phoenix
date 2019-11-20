package org.apache.phoenix.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ViewInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PhoenixMultiViewInputFormat<T extends Writable> extends InputFormat<NullWritable,T> {

    public PhoenixMultiViewInputFormat() {
    }

    @Override public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {

        final Configuration configuration = context.getConfiguration();
        final List<ViewInfo> viewsWithTTL = ViewTTLTool.getViewsWithTTL(configuration);
        return generateSplits(viewsWithTTL, configuration);
    }

    private List<InputSplit> generateSplits(List<ViewInfo> viewsWithTTL, Configuration configuration) {
        int numViewsInSplit = 10; // Should be gotten from configuration
        int mappers = viewsWithTTL.size() / numViewsInSplit + 1;
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(mappers);

        // Split the viewsWithTTL into splits

        for (int i = 0; i < mappers; i++) {
            psplits.add(new PhoenixMultiViewInputSplit(viewsWithTTL.subList(
                    i * numViewsInSplit, getUpperBound(numViewsInSplit, i, viewsWithTTL))));
        }

        return psplits;
    }

    private int getUpperBound(int numViewsInSplit, int i, List<ViewInfo> viewsWithTTL) {
        int upper = (i + 1) * numViewsInSplit;
        if (viewsWithTTL.size() < upper) {
            upper = viewsWithTTL.size();
        }

        return upper;
    }

    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();

        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return getPhoenixRecordReader(inputClass, configuration);
    }

    private RecordReader<NullWritable,T> getPhoenixRecordReader(Class<T> inputClass, Configuration configuration) {
        return new PhoenixMultiViewReader<>(inputClass , configuration);
    }
}
