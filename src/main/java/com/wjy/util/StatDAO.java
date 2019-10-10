package com.wjy.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class StatDAO {
    /**
     * 删除指定日期的数据
     */
    public static void  deleteData(String day ) {
        List<String> tables = Arrays.asList("day_video_access_topn_stat",
                "day_video_city_access_topn_stat",
                "day_video_traffics_topn_stat");

        Connection connection = null;
        PreparedStatement pstmt = null;

        try {
            connection = MySQLUtils.getConnection();

            for (String table : tables) {
                String deleteSQL = "delete from " + table + " where day = ?";
                pstmt = connection.prepareStatement(deleteSQL);
                pstmt.setString(1, day);
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            MySQLUtils.release(connection, pstmt);
        }
    }
}
