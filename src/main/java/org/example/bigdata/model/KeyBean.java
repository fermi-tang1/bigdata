package org.example.bigdata.model;

import lombok.*;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 15:36,2022/7/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class KeyBean implements WritableComparable<KeyBean> {
    private String phone;

    @Override
    public int compareTo(KeyBean o) {
        return this.phone.compareTo(o.phone);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.phone);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
    }
}
