package com.wjy.sxt1903.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 卡口状态
 * @author root
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MonitorState {
	
	private long taskId;
	private String normalMonitorCount;//正常的卡扣个数
	private String normalCameraCount;//正常的摄像头个数
	private String abnormalMonitorCount;//不正常的卡扣个数
	private String abnormalCameraCount;//不正常的摄像头个数
	private String abnormalMonitorCameraInfos;//不正常的摄像头详细信息

}
