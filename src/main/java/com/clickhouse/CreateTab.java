package com.clickhouse;

import java.sql.*;

public class CreateTab {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://192.168.244.101:39000");

        Statement statement = connection.createStatement();
//        statement.executeQuery("create database test");
//        create table test.jdbc_example(day Date, name String, age UInt8) Engine=Log
        statement.executeQuery("show tables");

    }
}

