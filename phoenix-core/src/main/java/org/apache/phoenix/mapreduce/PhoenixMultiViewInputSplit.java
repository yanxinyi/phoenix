package org.apache.phoenix.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.mapreduce.util.ViewInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PhoenixMultiViewInputSplit extends InputSplit implements Writable {

    List<ViewInfo> viewInfoList;

    public PhoenixMultiViewInputSplit() {
        this.viewInfoList = new ArrayList<>();
    }

    public PhoenixMultiViewInputSplit(List<ViewInfo> viewInfo /* and other params as needed */) {
        this.viewInfoList = viewInfo;
    }

    @Override public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, this.viewInfoList.size());
        for (ViewInfo viewInfo : this.viewInfoList) {
            WritableUtils.writeString(output, viewInfo.getTenantId());
            WritableUtils.writeString(output, viewInfo.getViewName());
            WritableUtils.writeVLong(output, viewInfo.getViewTtl());
        }
    }

    @Override public void readFields(DataInput input) throws IOException {
        int count = WritableUtils.readVInt(input);
        for (int i = 0; i < count; i++) {
            ViewInfo viewInfo = new ViewInfo(
                    WritableUtils.readString(input),
                    WritableUtils.readString(input),
                    WritableUtils.readVLong(input));
            this.viewInfoList.add(viewInfo);
        }
    }

    @Override public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public List<ViewInfo> getViewInfoList() {
        return this.viewInfoList;
    }
}
