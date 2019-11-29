package com.wjy.sxt1903.dao;

import com.wjy.sxt1903.entity.MonitorState;
import org.apache.ibatis.annotations.Insert;

public interface MonitorStateDAO {
    @Insert("insert into monitor_state (taskId,normal_monitor_count,normal_camera_count," +
            "abnormal_monitor_count,abnormal_camera_count,abnormal_monitor_camera_infos) " +
            "values (#{taskId},#{normalMonitorCount},#{normalCameraCount}," +
            "#{abnormalMonitorCount},#{abnormalCameraCount},#{abnormalMonitorCameraInfos})")
    void insert(MonitorState monitorState);
}
