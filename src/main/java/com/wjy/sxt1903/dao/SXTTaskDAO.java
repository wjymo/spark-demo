package com.wjy.sxt1903.dao;

import com.wjy.sxt1903.entity.SXTTask;
import com.wjy.zh13.project.entity.Task;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface SXTTaskDAO {

    @Insert("insert into task (task_name,create_time,start_time,finish_time,task_type,task_status,task_param) " +
            "values ('2xx','2xx','2xx','2xx','2xx','2xx','2xx')")
    void insert(SXTTask sxtTask);


    @Select("select * from task where task_id=#{taskId}")
    SXTTask findById(@Param("taskId") long taskId);
}
