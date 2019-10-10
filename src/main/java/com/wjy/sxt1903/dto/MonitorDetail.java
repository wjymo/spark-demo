package com.wjy.sxt1903.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class MonitorDetail implements Serializable {
    private static final long serialVersionUID = -6152117952056117780L;
    private Integer normalMonitorCount;
        private Integer normalCameraCount;
        private Integer abnormalMonitorCount;
        private Integer abnormalCameraCount;
        private String abnormalMonitorCameraInfos;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MonitorDetail that = (MonitorDetail) o;
        return Objects.equals(normalMonitorCount, that.normalMonitorCount) &&
                Objects.equals(normalCameraCount, that.normalCameraCount) &&
                Objects.equals(abnormalMonitorCount, that.abnormalMonitorCount) &&
                Objects.equals(abnormalCameraCount, that.abnormalCameraCount) &&
                Objects.equals(abnormalMonitorCameraInfos, that.abnormalMonitorCameraInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos);
    }

    public static void main(String[] args) {
        boolean equals = new MonitorDetail().equals(new MonitorDetail());
        System.out.println(1);
    }
}
