package com.wjy.util;

import com.wjy.sql.log.entity.DayVideoAccessStat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MySQLUtils {
    /**
     * 获取数据库连接
     */
    public static Connection getConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://172.168.1.108:33068/imooc_project?user=root&password=123456");
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 释放数据库连接等资源
     * @param connection
     * @param pstmt
     */
    public static void release(Connection connection, PreparedStatement pstmt)  {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch  (SQLException e) {
            e.printStackTrace();
        }   finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 批量保存DayVideoAccessStat到数据库
     */
    public static void insertDayVideoAccessTopN(List<DayVideoAccessStat> list) {

        Connection connection  = null;
        PreparedStatement pstmt  = null;

        try {
            connection = getConnection();

            connection.setAutoCommit(false); //设置手动提交

            String sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) ";
            pstmt = connection.prepareStatement(sql);

            for (DayVideoAccessStat ele:list) {
                pstmt.setString(1, ele.getDay());
                pstmt.setLong(2, ele.getCmsId());
                pstmt.setLong(3, ele.getTimes());
                pstmt.addBatch();
            }

            pstmt.executeBatch(); // 执行批量处理
            connection.commit();//手工提交
        }  catch (SQLException e) {
            e.printStackTrace();
        }  finally {
            MySQLUtils.release(connection, pstmt);
        }
    }
}
