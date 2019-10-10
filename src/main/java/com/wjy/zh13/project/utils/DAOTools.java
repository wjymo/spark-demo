package com.wjy.zh13.project.utils;

import com.wjy.zh13.project.dao.TaskDAO;
import com.wjy.zh13.project.entity.Task;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.Reader;

public class DAOTools {


    public static SqlSessionFactory sessionFactory;

    static {
        try {
            // 使用MyBatis提供的Resources类加载mybatis的配置文件
            Reader reader = Resources.getResourceAsReader("mybatis-config.xml");
            // 构建sqlSession的工厂
            sessionFactory = new SqlSessionFactoryBuilder().build(reader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建能执行映射文件中sql的sqlSession
    public static SqlSession getSession() {
        return sessionFactory.openSession();
    }
    public static void destroyConnection(SqlSession sqlSession) {
        sqlSession.close();
    }

    public static void main(String[] args) {
        SqlSession session = getSession();
        TaskDAO mapper = session.getMapper(TaskDAO.class);
        Task byId = mapper.findById(1);
        mapper.insert(null);
        session.commit();
        System.out.println(mapper);

    }
}
