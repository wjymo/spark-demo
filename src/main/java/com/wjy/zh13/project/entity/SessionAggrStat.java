package com.wjy.zh13.project.entity;

import lombok.Data;

/**
 * session聚合统计
 * @author Administrator
 *
 */
@Data
public class SessionAggrStat {
	
	private long taskid;
	private long session_count;
	private double visit_length_1s_3s_ratio;
	private double visit_length_4s_6s_ratio;
	private double visit_length_7s_9s_ratio;
	private double visit_length_10s_30s_ratio;
	private double visit_length_30s_60s_ratio;
	private double visit_length_1m_3m_ratio;
	private double visit_length_3m_10m_ratio;
	private double visit_length_10m_30m_ratio;
	private double visit_length_30m_ratio;
	private double step_length_1_3_ratio;
	private double step_length_4_6_ratio;
	private double step_length_7_9_ratio;
	private double step_length_10_30_ratio;
	private double step_length_30_60_ratio;
	private double step_length_60_ratio;
	

	
}
