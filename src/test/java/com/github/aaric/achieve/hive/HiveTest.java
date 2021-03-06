package com.github.aaric.achieve.hive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * HiveTest
 *
 * @author Aaric, created on 2018-07-02T11:18.
 * @since 0.1.0-SNAPSHOT
 */
public class HiveTest {

    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;

    @Before
    public void begin() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        conn = DriverManager.getConnection("jdbc:hive2://10.0.11.34:10000/dw");
    }

    @After
    public void end() throws Exception {
        if (null != ps) {
            ps.close();
        }
        if (null != rs) {
            rs.close();
        }
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String sql = "create database test_hive";
        ps = conn.prepareStatement(sql);
        ps.execute();
    }

    @Test
    public void testDropDatabase() throws Exception {
        String sql = "drop database test_hive";
        ps = conn.prepareStatement(sql);
        ps.execute();
    }

    @Test
    public void testShowDatabases() throws Exception {
        String sql = "show databases";
        ps = conn.prepareStatement(sql);
        rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        String sql = "create table test_hive.test_complex_data(aa array<int>, bb map<string, int>, cc struct<a:string, b:int, c:double>)";
        ps = conn.prepareStatement(sql);
        ps.execute();
    }

    @Test
    public void testDropTable() throws Exception {
        String sql = "drop table test_hive.test_complex_data";
        ps = conn.prepareStatement(sql);
        ps.execute();
    }

    @Test
    public void testShowTables() throws Exception {
        String sql = "show tables";
        ps = conn.prepareStatement(sql);
        rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void testImportToTable() throws Exception {
        String filePath = "/data/test_complex_data.txt"; //上传文件到hdfs
        String sql = "load data local inpath '" + filePath + "' into table test_hive.test_complex_data";
        ps = conn.prepareStatement(sql);
        ps.execute();
    }
}
