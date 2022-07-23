package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 14:28,2022/7/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NetWorkFlow implements WritableComparable<NetWorkFlow> {
    private Long uploadFlow;
    private Long downloadFlow;
    private Long totalFlow;

    @Override
    public int compareTo(NetWorkFlow o) {
        return Long.compare(o.getTotalFlow(), this.getTotalFlow());
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(this.uploadFlow);
        out.writeLong(this.downloadFlow);
        out.writeLong(this.totalFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uploadFlow = in.readLong();
        this.downloadFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    public String toString() {
        return uploadFlow + "\t " + downloadFlow + " \t" + totalFlow;
    }
}
