package model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 16:09,2022/7/22
 */
@Data
@Builder
public class Student {
    private String rowKey;
    private List<ColumnParam> columnParams;
}
