package model;

import lombok.Builder;
import lombok.Data;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 17:07,2022/7/22
 */
@Data
@Builder
public class ColumnParam{
    private String columnFamily;
    private String qualifier;
    private String value;
}
