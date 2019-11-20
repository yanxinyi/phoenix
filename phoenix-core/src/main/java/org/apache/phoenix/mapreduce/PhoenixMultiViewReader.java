package org.apache.phoenix.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.mapreduce.util.ViewInfo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PhoenixMultiViewReader<T extends Writable> extends
        RecordReader<NullWritable,T> {
    private Configuration  configuration;
    private Class<T> inputClass;
    Iterator<ViewInfo> it;

    public PhoenixMultiViewReader(final Class<T> inputClass, final Configuration configuration) {
        this.configuration = configuration;
        this.inputClass = inputClass;
    }

    @Override public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        final PhoenixMultiViewInputSplit pSplit = (PhoenixMultiViewInputSplit)split;
        final List<ViewInfo> viewInfo = pSplit.getViewInfoList();
        it = viewInfo.iterator();
    }

    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        return it.hasNext();
    }

    @Override public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override public T getCurrentValue() throws IOException, InterruptedException {
        ViewInfo currentValue = null;
        if (it.hasNext()) {
            currentValue = it.next();
        }
        return (T)currentValue;
    }

    @Override public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override public void close() throws IOException {

    }
}
