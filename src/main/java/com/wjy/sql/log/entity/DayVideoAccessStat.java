package com.wjy.sql.log.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class DayVideoAccessStat {
    private String day;
    private Long cmsId;
    private Long times;
}
