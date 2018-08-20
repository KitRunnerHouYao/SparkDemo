import java.sql.*;

/**
 * Created by yaohou on 14:33 2018/8/9.
 * description:
 */
public class ConnectionTest {
    private static String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static String CONNECTION_URL = "jdbc:phoenix:localhost:2181";
    private static Connection conn = null;
    static {
        try {
            Class.forName(JDBC_DRIVER);
        }catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    public static void initialConnection() {
        try {
            conn = DriverManager.getConnection(CONNECTION_URL);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            close();
        }
    }

    public static void close(){
        if(conn!=null){
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("phoenix 连接关闭失败");
            }
        }
    }
    public static void createTable(){
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS STUDENT(STUDENT_ID VARCHAR PRIMARY KEY,NAME VARCHAR,SEX VARCHAR)");
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            close();
        }
    }
    public static void insertValue(){
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("upsert into STUDENT values ('0001','hy','m')");
            statement.executeUpdate("upsert into STUDENT values ('0002','gll','f')");
            conn.commit();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            close();
        }
    }
    public static void searchTable(){
        try {
            PreparedStatement ps = conn.prepareStatement("select * from STUDENT");
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("STUDENT_ID"));
                System.out.println(rs.getString("NAME"));
                System.out.println(rs.getString(""));
            }
            rs.close();
            ps.close();
            System.out.println("game over!");
        } catch (SQLException e) {
            e.printStackTrace();
            close();
        }
    }

    public static void main(String[] args) {
        initialConnection();
        createTable();
        insertValue();
        searchTable();
        close();
    }

}
