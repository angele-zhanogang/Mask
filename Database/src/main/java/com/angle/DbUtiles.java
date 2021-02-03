package com.angle;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class DbUtiles {
    private static Connection conn;

    public static Connection getConnection() {
        String url = "jdbc:mysql://hadoop:25336/channel?characterEncoding=utf-8&useSSL=false";
        String driver = "com.mysql.jdbc.Driver";
        String user = "channel";
        String passwd = "1qaz@WSX";
        DbUtils.loadDriver(driver);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, passwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    @Test
    public void query() {
        conn = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        List<Map<String, Object>> list = null;
        try {
            list = queryRunner.query(conn, "select * from channel.employee", new MapListHandler());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(list != null);
        if (list != null) {
            for (Map<String, Object> obj : list) {
                System.out.println("id:" + obj.get("id"));
                System.out.println("first_name:" + obj.get("first_name"));
                System.out.println("last_name:" + obj.get("last_name"));
                System.out.println("salary:" + obj.get("salary"));
            }
        }
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insert() {
        conn = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        String sql = "insert into employee(first_name,last_name,salary) values (?,?,?)";
        Object[] params = {"zhangsan", "li", 2200};
        try {
            //todo:关闭自动提交
            conn.setAutoCommit(false);
            int update = queryRunner.update(conn, sql, params);
            //todo:提交
            conn.commit();
        } catch (SQLException throwables) {
            //todo:回滚事务
            DbUtils.rollbackAndCloseQuietly(conn);
            throwables.printStackTrace();
        } finally {
            //todo:关闭资源
            DbUtils.closeQuietly(conn);
        }
    }

    @Test
    public void delete() {
        conn = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        String sql = "delete from employee where id = ?";
        try {
            conn.setAutoCommit(false);
            queryRunner.update(conn, sql, 7);
            conn.commit();
        } catch (SQLException e) {
            DbUtils.rollbackAndCloseQuietly(conn);
            e.printStackTrace();
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }

    @Test
    public void update() {
        conn = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        String sql = "update employee set salary = ? where id = ?";
        List<Object> params = new ArrayList<>();
        params.add(4000);
        params.add(6);
        try {
            conn.setAutoCommit(false);
            //todo:参数必须用数组传入params.toArray()
            queryRunner.update(conn, sql, params.toArray());
            conn.commit();
        } catch (SQLException e) {
            DbUtils.rollbackAndCloseQuietly(conn);
            e.printStackTrace();
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }

    @Test
    @SuppressWarnings("uncheck")
    public void queryToEmployee() {
        conn = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        String sql = "select * from employee";
        List<Employee1> list = null;
        try {
            //todo:bean类变量名 必须和数据库中字段名称完全一致
            list = queryRunner.query(conn, sql, new BeanListHandler<>(Employee1.class));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DbUtils.closeQuietly(conn);
        }
        if (list != null) {
            for (Employee1 employee1 : list) {
                System.out.println(employee1.toString());
            }
        }
    }

    /**
     * todo:批量插入
     */
    @Test
    public void batch() {
        conn = getConnection();
        QueryRunner queryRunner = new QueryRunner();
        String sql = "insert into employee (first_name,last_name,salary) values (?,?,?)";
        ArrayList<Employee1> list = new ArrayList<>();
        list.add(new Employee1("ww", "ww", 300));
        list.add(new Employee1("qq", "qq", 300));
        list.add(new Employee1("ee", "ee", 300));
        DbUtiles db = new DbUtiles();
        //todo:二维数组，高维代表sql执行插入的次数，低维代表每次插入参数个数
        Object[][] params = db.ListToTwoArray(list);
        try {
            conn.setAutoCommit(false);
            queryRunner.batch(conn, sql, params);
            conn.commit();
        } catch (SQLException e) {
            DbUtils.rollbackAndCloseQuietly(conn);
            e.printStackTrace();
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }

    public Object[][] ListToTwoArray(List<Employee1> list) {
        Object[][] params = new Object[list.size()][3];
        Object[] arr = list.toArray();
        for (int i = 0; i < list.size(); i++) {
            Employee1 e = list.get(i);
            params[i][0] = e.getFirst_name();
            params[i][1] = e.getLast_name();
            params[i][2] = e.getSalary();
        }
        return params;
    }
}
