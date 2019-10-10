package com.wjy.zh13.project.dao;

import com.wjy.zh13.project.entity.Task;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface TaskDAO {

    @Insert("insert into task (task_name,create_time,start_time,finish_time,task_type,task_status,task_param) " +
            "values ('2xx','2xx','2xx','2xx','2xx','2xx','2xx')")
    void insert(Task task);


    @Select("select * from task where task_id=#{taskId}")
    Task findById(@Param("taskId")long taskId);
}
