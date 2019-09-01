import org.apache.commons.dbcp2.BasicDataSource;

import javax.sound.midi.SysexMessage;
import javax.sql.DataSource;
import java.sql.*;

/**
 * Author: zhuxiaoxiang
 * Date: 2019/7/28 19:31
 */
public class TestConnection {
    public static void main(String[] args) throws Exception{
        getConnection1();
        System.exit(-1);
        BasicDataSource dataSource = new BasicDataSource();
        System.out.println("open........");
        Connection connection = getConnection(dataSource);
        String sql = "insert into person(name,score) values(?, ?);";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, "abc");
        ps.setInt(2, 20);
        ps.addBatch();
        int[] s=ps.executeBatch();
        System.out.println("size:"+s.length);
        connection.close();
    }
    public static Connection getConnection(BasicDataSource dataSource) {
        //dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://172.0.0.1:3306/mytest?autoReconnect=true?useSSL=true");
        dataSource.setUsername("xqhadoop@%");
        dataSource.setPassword("xqhadoop");
        //设置连接池的一些参数
        //dataSource.setInitialSize(10);
        //dataSource.setMaxTotal(50);
        //dataSource.setMinIdle(2);
        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
    public static void getConnection1(){
        Connection con;
        String driver="com.mysql.cj.jdbc.Driver";
        //这里我的数据库是qcl
        String url="jdbc:mysql://localhost:3306/mytest?useSSL=true";
        String user="xqhadoop";
        String password="xqhadoop";
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed()) {
                System.out.println("数据库连接成功");
            }
            Statement statement = con.createStatement();
            String sql = "select * from student;";//我的表格叫home
            ResultSet resultSet = statement.executeQuery(sql);
            String name;
            while (resultSet.next()) {
                name = resultSet.getString("name");
                System.out.println("姓名：" + name);
            }
            resultSet.close();
            con.close();
        } catch (ClassNotFoundException e) {
            System.out.println("数据库驱动没有安装");

        } catch (SQLException e) {
            System.out.println(e);
            System.out.println("数据库连接失败");
        }
    }
}
