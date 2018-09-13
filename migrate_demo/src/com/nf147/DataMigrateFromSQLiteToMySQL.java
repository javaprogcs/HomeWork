package com.nf147;

import java.sql.*;

public class DataMigrateFromSQLiteToMySQL {
    private Connection getSQLiteConnection() throws Exception {
        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection("jdbc:sqlite:d:/data/lagou.db");
    }

    private Connection getMySQLConnection() throws Exception {
        Class.forName("org.mariadb.jdbc.Driver"); // 注意 rewriteBatchedStatements 参数！
        return DriverManager.getConnection("jdbc:mariadb://127.0.0.1:3306/lagou?rewriteBatchedStatements=true", "root", "xxx");
    }

    private void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ignored) {
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException ignored) {
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    public void doMigrate() {
        // 数据从哪里来？
        Connection liteConn = null;
        Statement liteSt = null;
        ResultSet liteRs = null;

        // 数据到哪里去？
        Connection mysqlConn = null;
        PreparedStatement mysqlPs = null;

        try {
            // 数据从这里来
            liteConn = this.getSQLiteConnection();
            liteSt = liteConn.createStatement();
            liteRs = liteSt.executeQuery("select * from lagou_position");

            // 数据到这里去
            mysqlConn = this.getMySQLConnection();
            mysqlPs = mysqlConn.prepareStatement("insert into lagou_p1 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            // 关闭自动提交，也就是开启事务
            mysqlConn.setAutoCommit(false);

            int i = 0;
            long startTime = System.currentTimeMillis(); // 计时开始

            while(liteRs.next()) {
                // 数据的转移
                for (int j = 1; j < 20; j++) {
                    mysqlPs.setObject(j, liteRs.getObject(j));
                }
                mysqlPs.addBatch();

                // 每 1000 条，向数据库发送一次执行请求
                if (i++ % 10000 == 0) {
                    mysqlPs.executeBatch();
                }
            }

            mysqlPs.executeBatch();
            mysqlConn.commit();  // 提交事务

            long stopTime = System.currentTimeMillis(); // 计时结束

            // 输出结果
            System.out.println("总共多少数据:" + i);
            System.out.println("一共花费 " + (stopTime - startTime) / 1000.0 + " 秒");
        } catch (Exception var14) {
            try {
                if (mysqlConn != null) {
                    mysqlConn.rollback();
                }
            } catch (SQLException ignored) {
            }
        } finally {
            this.release(liteConn, liteSt, liteRs);
            this.release(mysqlConn, mysqlPs, null);
        }
    }

    public static void main(String[] args) {
        DataMigrateFromSQLiteToMySQL migrate = new DataMigrateFromSQLiteToMySQL();
        migrate.doMigrate();
    }
}
