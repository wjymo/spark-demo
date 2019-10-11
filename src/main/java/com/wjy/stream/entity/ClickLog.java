package com.wjy.stream.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClickLog implements Serializable {
    private static final long serialVersionUID = -6386095803042578054L;
    private String ip;
    private Integer courseId;
    private Integer statusCode;
    private String referer;
    private String time;
}
