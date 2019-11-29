package com.wjy.sxt1903.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 获取车流量排名前N的卡口
 * @author root
 *
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopNMonitor2CarCount {
	private long taskId;
	private String monitorId;
	private int carCount;
	 

}
