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
		//机构相关信息建表
		createMEMBER_SKETCH_INFO();
		//质押物加权价相关
//		createMEMBER_PRICE_DEAL_INFO();
//		createCTGRY_PRICE_DEAL_INFO();
//		createGLOBAL_PRICE_DEAL_INFO();
//		System.out.println("质押物加权价相关表创建结束");
//		//质押物接受度相关表
//		createMEMBER_BOND_CTGRY_DEAL_INFO();
//		createCTGRY_BOND_CTGRY_DEAL_INFO();
//		createMEMBER_BOND_RTNG_DEAL_INFO();
//		createCTGRY_BOND_RTNG_DEAL_INFO();
//		System.out.println("质押物接受度相关表创建结束");
//		//质押物折扣率走势图
//		createMEMBER_BOND_TYPE_DEAL_INFO();
//		createCTGRY_BOND_TYPE_DEAL_INFO();
//		createMEMBER_BOND_ISSR_DEAL_INFO();
//		createCTGRY_BOND_ISSR_DEAL_INFO();
//		System.out.println("质押物折扣率走势图相关表创建结束");
//		//交易分布展示
//		createMEMBER_CTGRY_AMOUNT_DEAL_INFO();
//		createCTGRY_CTGRY_AMOUNT_DEAL_INFO();
//		createMEMBER_CTGRY_NUMBER_DEAL_INFO();
//		System.out.println("交易分布展示相关表创建结束");
	}

	public static void createMEMBER_SKETCH_INFO(){
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_SKETCH_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL PRIMARY KEY, FULL_NM VARCHAR(300), "
					+ "INS_SHOW_NAME VARCHAR(300), BOND_AFTER_SHOW_NAME VARCHAR(300))");
			System.out.println("create Member_SKETCH_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_SKETCH_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS MEMBER_SKETCH_INFO_index on MEMBER_SKETCH_INFO (" +
					"UNQ_ID_IN_SRC_SYS) INCLUDE(FULL_NM,INS_SHOW_NAME,BOND_AFTER_SHOW_NAME)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createMEMBER_PRICE_DEAL_INFO() {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS MEMBER_PRICE_DEAL_INFO"
					+ "(UNQ_ID_IN_SRC_SYS VARCHAR(80) NOT NULL, JOIN_TYPE VARCHAR(40) NOT NULL, "
					+ "PD_CD VARCHAR (40) NOT NULL,DEAL_DT DATE NOT NULL, "
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
					+ "PD_CD VARCHAR (40) NOT NULL,DEAL_DT DATE NOT NULL, "
					+ "REPO_WEIGHTED_PRICE DECIMAL,RVRSE_WEIGHTED_PRICE DECIMAL,"
					+ "PRICE_DIFFERENCE DECIMAL,"
					+ "REPO_MAX_DEAL_REPO_RATE DECIMAL, REPO_MIN_DEAL_REPO_RATE DECIMAL,"
					+ "RVRSE_MAX_DEAL_REPO_RATE DECIMAL, RVRSE_MIN_DEAL_REPO_RATE DECIMAL,"
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
					+ "(PD_CD VARCHAR (40) NOT NULL,DEAL_DT DATE NOT NULL, "
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
					+ "BOND_CTGRY_NM VARCHAR (100) NOT NULL,DEAL_DT DATE NOT NULL, "
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
					+ "BOND_CTGRY_NM VARCHAR (100) NOT NULL,DEAL_DT DATE NOT NULL,"
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
					+ "BOND_CTGRY_NM VARCHAR(100) NOT NULL,  RTNG_DESC VARCHAR (512) NOT NULL,DEAL_DT DATE NOT NULL, "
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (UNQ_ID_IN_SRC_SYS,JOIN_TYPE,BOND_CTGRY_NM,"
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
					+ "(INS_SHOW_NAME VARCHAR(300) NOT NULL, BOND_CTGRY_NM VARCHAR(100) NOT NULL,"
					+ "RTNG_DESC VARCHAR (512) NOT NULL,DEAL_DT DATE NOT NULL, "
					+ "REPO_SUM_AMOUNT DECIMAL,RVRSE_SUM_AMOUNT DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (INS_SHOW_NAME, BOND_CTGRY_NM,"
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
					+ "RTNG_DESC VARCHAR (512) NOT NULL,DEAL_DT DATE NOT NULL,"
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
					+ "RTNG_DESC VARCHAR (512) NOT NULL, DEAL_DT DATE NOT NULL,"
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
					+ "ISSR_NM VARCHAR (300) NOT NULL,DEAL_DT DATE NOT NULL,"
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
					+ "ISSR_NM VARCHAR (300) NOT NULL,DEAL_DT DATE NOT NULL,"
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
					+ "COU_PARTY_INS_SHOW_NAME VARCHAR (300) NOT NULL,DEAL_DT DATE NOT NULL,"
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
					+ "COU_PARTY_INS_SHOW_NAME VARCHAR (300) NOT NULL, DEAL_DT DATE NOT NULL,"
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
					+ "COU_PARTY_INS_SHOW_NAME VARCHAR NOT NULL,DEAL_DT DATE NOT NULL,"
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

	public static  void dropTables(){
		String []sqls = new String[14];
		sqls[0] = "drop table MEMBER_PRICE_DEAL_INFO";
		sqls[1] = "Drop table CTGRY_PRICE_DEAL_INFO";
		sqls[2] = "Drop table GLOBAL_PRICE_DEAL_INFO";
		sqls[3] = "Drop table MEMBER_BOND_CTGRY_DEAL_INFO";
		sqls[4] = "Drop table CTGRY_BOND_CTGRY_DEAL_INFO";
		sqls[5] = "Drop table MEMBER_BOND_RTNG_DEAL_INFO"; //
		sqls[6] = "Drop table CTGRY_BOND_RTNG_DEAL_INFO"; //
		sqls[7] = "Drop table MEMBER_BOND_TYPE_DEAL_INFO";
		sqls[8] = "Drop table CTGRY_BOND_TYPE_DEAL_INFO";
		sqls[9] = "Drop table MEMBER_BOND_ISSR_DEAL_INFO";
		sqls[10] = "Drop table CTGRY_BOND_ISSR_DEAL_INFO";
		sqls[11] = "Drop table MEMBER_CTGRY_AMOUNT_DEAL_INFO";
		sqls[12] = "Drop table CTGRY_CTGRY_AMOUNT_DEAL_INFO";
		sqls[13] = "Drop table MEMBER_CTGRY_NUMBER_DEAL_INFO";

		for(int i=0;i<sqls.length;i++){
			try{
				PreparedStatement pstmt;
				pstmt = conn.prepareStatement(sqls[i]);
				pstmt.execute();
			}catch (Exception e){
				e.printStackTrace();
				close();
			}
		}
	}

	public static void dropTable(String tableName){
		PreparedStatement pstmt;
		String dropString  = "Drop table "+tableName;
		try{
			pstmt = conn.prepareStatement(dropString);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void dropIndex(String tableName,String indexName){
		PreparedStatement pstmt;
		String dropString  = "Drop index "+indexName +" on "+tableName;
		try{
			pstmt = conn.prepareStatement(dropString);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndexes(){
		createIndex4MEMBER_SKETCH_INFO();
		//
		createIndex4MEMBER_PRICE_DEAL_INFO();
		createIndex4CTGRY_PRICE_DEAL_INFO();
		createIndex4GLOBAL_PRICE_DEAL_INFO();
		System.out.println("LOCAL INDEX FOR FIRST 3 TABLES succeed!");
		createIndex4MEMBER_BOND_CTGRY_DEAL_INFO();
		createIndex4CTGRY_BOND_CTGRY_DEAL_INFO();
		createIndex4MEMBER_BOND_RTNG_DEAL_INFO();
		createIndex4CTGRY_BOND_RTNG_DEAL_INFO();
		System.out.println("CREATE GLOBAL INDEXES FOR SECOND GROUP SERVICE succeed!");
		createIndex4MEMBER_BOND_TYPE_DEAL_INFO();
		createIndex4CTGRY_BOND_TYPE_DEAL_INFO();
		createIndex4MEMBER_BOND_ISSR_DEAL_INFO();
		createIndex4CTGRY_BOND_ISSR_DEAL_INFO();
		System.out.println("CREATE GLOBAL INDEXES FOR THIRD GROUP SERVICE succeed!");
		createIndex4MEMBER_CTGRY_AMOUNT_DEAL_INFO();
		createIndex4CTGRY_CTGRY_AMOUNT_DEAL_INFO();
		createIndex4MEMBER_CTGRY_NUMBER_DEAL_INFO();
		System.out.println("CREATE  GLOBAL INDEXES FOR FOURTH GROUP SERVICE succeed!");
	}

	public static void createIndex4MEMBER_PRICE_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS  G1_mem_index on MEMBER_PRICE_DEAL_INFO (DEAL_DT,UNQ_ID_IN_SRC_SYS,JOIN_TYPE,PD_CD)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4CTGRY_PRICE_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS G1_ctgry_index on CTGRY_PRICE_DEAL_INFO (DEAL_DT,INS_SHOW_NAME,PD_CD)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4GLOBAL_PRICE_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS G1_global_index on GLOBAL_PRICE_DEAL_INFO (DEAL_DT,PD_CD)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_BOND_CTGRY_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G2_mem_Bond_ctgry_index on MEMBER_BOND_CTGRY_DEAL_INFO (" +
					"DEAL_DT,UNQ_ID_IN_SRC_SYS,JOIN_TYPE,BOND_CTGRY_NM) include(REPO_SUM_AMOUNT,RVRSE_SUM_AMOUNT)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4CTGRY_BOND_CTGRY_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G2_CTGRY_bond_ctgry_index on CTGRY_BOND_CTGRY_DEAL_INFO (" +
					"DEAL_DT,INS_SHOW_NAME,BOND_CTGRY_NM) include (REPO_SUM_AMOUNT,RVRSE_SUM_AMOUNT)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_BOND_RTNG_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G2_mem_Bond_rtng_index on MEMBER_BOND_RTNG_DEAL_INFO (" +
					"DEAL_DT,UNQ_ID_IN_SRC_SYS,JOIN_TYPE, BOND_CTGRY_NM, RTNG_DESC) include (REPO_SUM_AMOUNT,RVRSE_SUM_AMOUNT)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4CTGRY_BOND_RTNG_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G2_CTGRY_bond_RTNG_index on CTGRY_BOND_RTNG_DEAL_INFO (" +
					"DEAL_DT,INS_SHOW_NAME,BOND_CTGRY_NM,RTNG_DESC) include (REPO_SUM_AMOUNT,RVRSE_SUM_AMOUNT)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_BOND_TYPE_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G3_mem_Bond_TYPE_index on MEMBER_BOND_TYPE_DEAL_INFO (" +
					"DEAL_DT,UNQ_ID_IN_SRC_SYS,JOIN_TYPE, BOND_CTGRY_NM,RTNG_DESC) include(REPO_WEIGHTED_CNVRSN_PRPRTN," +
					"RVRSE_WEIGHTED_CNVRSN_PRPRTN)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4CTGRY_BOND_TYPE_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create  index IF NOT EXISTS G3_CTGRY_bond_TYPE_index on CTGRY_BOND_TYPE_DEAL_INFO (" +
					"DEAL_DT,INS_SHOW_NAME,BOND_CTGRY_NM,RTNG_DESC) include (REPO_WEIGHTED_CNVRSN_PRPRTN," +
					"RVRSE_WEIGHTED_CNVRSN_PRPRTN)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_BOND_ISSR_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G3_mem_Bond_ISSR_index on MEMBER_BOND_ISSR_DEAL_INFO (" +
					"DEAL_DT,UNQ_ID_IN_SRC_SYS,JOIN_TYPE, ISSR_NM) include (REPO_WEIGHTED_CNVRSN_PRPRTN," +
					"RVRSE_WEIGHTED_CNVRSN_PRPRTN)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4CTGRY_BOND_ISSR_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G3_CTGRY_bond_ISSR_index on CTGRY_BOND_ISSR_DEAL_INFO (" +
					"DEAL_DT,INS_SHOW_NAME,ISSR_NM) include (REPO_WEIGHTED_CNVRSN_PRPRTN,RVRSE_WEIGHTED_CNVRSN_PRPRTN)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_CTGRY_AMOUNT_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G4_mem_CTGRY_AMOUNT_index on MEMBER_CTGRY_AMOUNT_DEAL_INFO (" +
					"DEAL_DT,UNQ_ID_IN_SRC_SYS,JOIN_TYPE)  include (REPO_SUM_AMOUNT,RVRSE_SUM_AMOUNT)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4CTGRY_CTGRY_AMOUNT_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create index IF NOT EXISTS G4_CTGRY_CTGRY_AMOUNT_index on CTGRY_CTGRY_AMOUNT_DEAL_INFO (" +
					"DEAL_DT,INS_SHOW_NAME,COU_PARTY_INS_SHOW_NAME) include (REPO_SUM_AMOUNT,RVRSE_SUM_AMOUNT)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}

	public static void createIndex4MEMBER_CTGRY_NUMBER_DEAL_INFO(){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS G4_MEMBER_CTGRY_NUMBER_index on MEMBER_CTGRY_NUMBER_DEAL_INFO (" +
					"DEAL_DT,INS_SHOW_NAME,UNQ_ID_IN_SRC_SYS,JOIN_TYPE,COU_PARTY_INS_SHOW_NAME)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
			close();
		}
	}


	public static void main(String[] args) {
		// TODO Auto-generated method stub

		initialConnection();
//		createMEMBER_BOND_RTNG_DEAL_INFO();
//		createCTGRY_BOND_RTNG_DEAL_INFO();
//		createIndex4CTGRY_BOND_RTNG_DEAL_INFO();
//		createIndex4MEMBER_BOND_RTNG_DEAL_INFO();
		//	dropIndex("MEMBER_BOND_RTNG_DEAL_INFO","G2_mem_Bond_rtng_index");
	//	dropIndex("CTGRY_BOND_RTNG_DEAL_INFO","G2_CTGRY_bond_RTNG_index");
	//	dropTable("MEMBER_BOND_RTNG_DEAL_INFO");
	//	dropTable("CTGRY_BOND_RTNG_DEAL_INFO");
	//	dropTables();
		createTables();
//		createIndexes();
		close();
	}

}
