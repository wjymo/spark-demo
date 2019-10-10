package com.wjy.zh13.project.dao;

import com.wjy.zh13.project.entity.SessionAggrStat;
import org.apache.ibatis.annotations.Insert;

public interface SessionAggrStatDAO {
    @Insert("insert into session_aggr_stat" +
            "(`task_id`,`session_count`,`1s_3s`,`4s_6s`,`7s_9s`,`10s_30s`,`30s_60s`," +
            "`1m_3m`,`3m_10m`,`10m_30m`,`30m`,`1_3`,`4_6`,`7_9`,`10_30`,`30_60`,`60`)" +
            " values (#{taskid},#{session_count},#{visit_length_1s_3s_ratio},#{visit_length_4s_6s_ratio}," +
            "#{visit_length_7s_9s_ratio},#{visit_length_10s_30s_ratio},#{visit_length_30s_60s_ratio}," +
            "#{visit_length_1m_3m_ratio},#{visit_length_3m_10m_ratio},#{visit_length_10m_30m_ratio}," +
            "#{visit_length_30m_ratio},#{step_length_1_3_ratio},#{step_length_4_6_ratio},#{step_length_7_9_ratio}," +
            "#{step_length_10_30_ratio},#{step_length_30_60_ratio},#{step_length_60_ratio})")
    void insert(SessionAggrStat sessionAggrStat);


}
