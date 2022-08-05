package org.example.bigdata.model.VO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 10:42,2022/8/5
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MovieVO {
    private String movieName;
    private String avgRate;
}
