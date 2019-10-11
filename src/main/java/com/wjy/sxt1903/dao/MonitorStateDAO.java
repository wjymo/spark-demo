package com.wjy.sxt1903.dao;

import com.wjy.sxt1903.entity.MonitorState;
import org.apache.ibatis.annotations.Insert;

public interface MonitorStateDAO {
    @Insert("insert into monitor_state ")
    void insert(MonitorState monitorState);
}
