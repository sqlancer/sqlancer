package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import sqlancer.Main;
import sqlancer.Randomly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestSparkSQLTLP {
    private final String host = "172.17.0.3";
    private final String port = "10000";
    private final String username = "sqlancer";
    private final String password = "sqlancer";

    @Test
    public void testSparkSQLTLP() {
        String url = "jdbc:hive2://172.17.0.2:10000/randgen_agg_1731764089?SSL=true"; // 修改为您的 Thrift Server 地址

        // 连接对象
        Connection connection = null;

        try {
            // 加载 JDBC 驱动
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            // 创建连接
            connection = DriverManager.getConnection(url);

            // 创建语句
            Statement statement = connection.createStatement();

            // 执行查询
            String query = "SHOW TABLES"; // 示例查询
            ResultSet resultSet = statement.executeQuery(query);

            // 处理结果
            while (resultSet.next()) {
                String tableName = resultSet.getString(2);
                System.out.println("Table: " + tableName);
            }

            // 关闭结果集和语句
            resultSet.close();
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 确保连接被关闭
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
