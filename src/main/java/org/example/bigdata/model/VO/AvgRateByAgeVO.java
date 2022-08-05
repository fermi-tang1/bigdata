package org.example.bigdata.model.VO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 09:11,2022/8/5
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AvgRateByAgeVO {
    private String age;
    private String rate;
}
