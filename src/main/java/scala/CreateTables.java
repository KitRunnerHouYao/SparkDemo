package scala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CreateTables {
	
	private static String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static String CONNECTION_URL = "jdbc:phoenix:127.0.0.1:2181";
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
	
	public static void createTables() {
		//质押物加权价相关
		createMEMBER_PRICE_DEAL_INFO();
		createCTGRY_PRICE_DEAL_INFO();
		createGLOBAL_PRICE_DEAL_INFO();
		System.out.println("质押物加权价相关表创建结束");
		//质押物接受度相关表
		createMEMBER_BOND_CTGRY_DEAL_INFO();
		createCTGRY_BOND_CTGRY_DEAL_INFO();
		createMEMBER_BOND_RTNG_DEAL_INFO();
		createCTGRY_BOND_RTNG_DEAL_INFO();
		System.out.println("质押物接受度相关表创建结束");
		//质押物折扣率走势图
		createMEMBER_BOND_TYPE_DEAL_INFO();
		createCTGRY_BOND_TYPE_DEAL_INFO();
		createMEMBER_BOND_ISSR_DEAL_INFO();
		createCTGRY_BOND_ISSR_DEAL_INFO();
		System.out.println("质押物折扣率走势图相关表创建结束");
		//交易分布展示
		createMEMBER_CTGRY_AMOUNT_DEAL_INFO();
		createCTGRY_CTGRY_AMOUNT_DEAL_INFO();
		createMEMBER_CTGRY_NUMBER_DEAL_INFO();
		System.out.println("交易分布展示相关表创建结束");
	}
	public static void createMEMBER_PRICE_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_PRICE_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL, "
					+ "PD_CD VARCHAR (40) NOT NULL,DEAL_DT VARCHAR NOT NULL, "
					+ "REPO_WEIGHTED_PRICE DECIMAL, REPO_SUM_AMOUNT DECIMAL,"
					+ "RVRSE_WEIGHTED_PRICE DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "PRICE_DIFFERENCE DECIMAL, TOTAL_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "PD_CD,DEAL_DT))");
			System.out.println("create MEMBER_PRICE_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}

	public static void createCTGRY_PRICE_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS CTGRY_PRICE_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,"
					+ "PD_CD VARCHAR (40) NOT NULL,DEAL_DT VARCHAR NOT NULL, "
					+ "REPO_WEIGHTED_PRICE DECIMAL,RVRSE_WEIGHTED_PRICE DECIMAL,"
					+ "MAX_DEAL_REPO_RATE DECIMAL, MIN_DEAL_REPO_RATE DECIMAL,"
					+ "PRICE_DIFFERENCE DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,PD_CD,DEAL_DT))");
			pstmt.execute();
			pstmt.close();
			System.out.println("create CTGRY_PRICE_DEAL_INFO succeed!");
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}
	}

	public static void createGLOBAL_PRICE_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS GLOBAL_PRICE_DEAL_INFO"
					+ "(PD_CD VARCHAR (40) NOT NULL,DEAL_DT VARCHAR NOT NULL, "
					+ "BANKS_WEIGHTED_PRICE DECIMAL,BANKS_MAX_DEAL_REPO_RATE DECIMAL,"
					+ "BANKS_MIN_DEAL_REPO_RATE DECIMAL, MARKET_WEIGHTED_PRICE DECIMAL,"
					+ "MARKET_MAX_DEAL_REPO_RATE DECIMAL, MARKET_MIN_DEAL_REPO_RATE DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (PD_CD,DEAL_DT))");
			pstmt.execute();
			pstmt.close();
			System.out.println("create CTGRY_PRICE_DEAL_INFO succeed!");
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}
	}
	
	public static void createMEMBER_BOND_CTGRY_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_BOND_CTGRY_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL, "
					+ "BOND_CTGRY_NM VARCHAR (100) NOT NULL,DEAL_DT VARCHAR NOT NULL, "
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "BOND_CTGRY_NM,DEAL_DT))");
			System.out.println("create MEMBER_BOND_CTGRY_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	
	public static void createCTGRY_BOND_CTGRY_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS CTGRY_BOND_CTGRY_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,"
					+ "BOND_CTGRY_NM VARCHAR (100) NOT NULL,DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,"
					+ "BOND_CTGRY_NM,DEAL_DT))");
			System.out.println("create CTGRY_BOND_CTGRY_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	
	public static void createMEMBER_BOND_RTNG_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_BOND_RTNG_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL,"
					+ "RTNG_DESC VARCHAR (512) NOT NULL,DEAL_DT VARCHAR NOT NULL, "
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "RTNG_DESC,DEAL_DT))");
			System.out.println("create MEMBER_BOND_RTNG_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	
	public static void createCTGRY_BOND_RTNG_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS CTGRY_BOND_RTNG_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,"
					+ "RTNG_DESC VARCHAR (512) NOT NULL,DEAL_DT VARCHAR NOT NULL, "
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,"
					+ "RTNG_DESC,DEAL_DT))");
			System.out.println("create CTGRY_BOND_RTNG_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	//质押物折扣率走势图
	public static void createMEMBER_BOND_TYPE_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_BOND_TYPE_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL,"
					+ "BOND_CTGRY_NM VARCHAR (100) NOT NULL,"
					+ "RTNG_DESC VARCHAR (512) NOT NULL,DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_WEIGHTED_CNVRSN_PRPRTN DECIMAL, REPO_SUM_AMOUNT DECIMAL,"
					+ "RVRSE_WEIGHTED_CNVRSN_PRPRTN DECIMAL, RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "BOND_CTGRY_NM,RTNG_DESC,DEAL_DT))");
			System.out.println("create MEMBER_BOND_TYPE_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}

	public static void createCTGRY_BOND_TYPE_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS CTGRY_BOND_TYPE_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,"
					+ "BOND_CTGRY_NM VARCHAR (100) NOT NULL,"
					+ "RTNG_DESC VARCHAR (512) NOT NULL, DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_WEIGHTED_CNVRSN_PRPRTN DECIMAL,"
					+ "RVRSE_WEIGHTED_CNVRSN_PRPRTN DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,BOND_CTGRY_NM,"
					+ "RTNG_DESC,DEAL_DT))");
			System.out.println("create CTGRY_BOND_TYPE_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	
	public static void createMEMBER_BOND_ISSR_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_BOND_ISSR_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL,"
					+ "ISSR_NM VARCHAR (300) NOT NULL,DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_WEIGHTED_CNVRSN_PRPRTN DECIMAL, REPO_SUM_AMOUNT DECIMAL,"
					+ "RVRSE_WEIGHTED_CNVRSN_PRPRTN DECIMAL, RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "ISSR_NM,DEAL_DT))");
			System.out.println("create MEMBER_BOND_ISSR_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}

	public static void createCTGRY_BOND_ISSR_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS CTGRY_BOND_ISSR_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,"
					+ "ISSR_NM VARCHAR (300) NOT NULL,DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_WEIGHTED_CNVRSN_PRPRTN DECIMAL,"
					+ "RVRSE_WEIGHTED_CNVRSN_PRPRTN DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,ISSR_NM,DEAL_DT))");
			System.out.println("create CTGRY_BOND_ISSR_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
 	//交易分布展示
	public static void createMEMBER_CTGRY_AMOUNT_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_CTGRY_AMOUNT_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL,"
					+ "COU_PARTY_INS_SHOW_NAME VARCHAR (300) NOT NULL,DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "COU_PARTY_INS_SHOW_NAME,DEAL_DT))");
			System.out.println("create MEMBER_CTGRY_AMOUNT_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	
	public static void createCTGRY_CTGRY_AMOUNT_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS CTGRY_CTGRY_AMOUNT_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,"
					+ "COU_PARTY_INS_SHOW_NAME VARCHAR (300) NOT NULL, DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,COU_PARTY_INS_SHOW_NAME,"
					+ "DEAL_DT))");
			System.out.println("create CTGRY_CTGRY_AMOUNT_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}
	
	public static void createMEMBER_CTGRY_NUMBER_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_CTGRY_NUMBER_DEAL_INFO"
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL,UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL,"
					+ "JOIN_TYPE VARCHAR(40) NOT NULL,"
					+ "COU_PARTY_INS_SHOW_NAME VARCHAR NOT NULL,DEAL_DT VARCHAR NOT NULL,"
					+ "REPO_COU_PARTY_UNIQ_SET VARCHAR,RVRSE_COU_PARTY_UNIQ_SET VARCHAR,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME,UNQ_ID_IN_SRC_SYS,JOIN_TYPE,"
					+ "COU_PARTY_INS_SHOW_NAME,DEAL_DT))");
			System.out.println("create MEMBER_CTGRY_NUMBER_DEAL_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}	
	}	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		initialConnection();
		createTables();
		close();
	}

}
