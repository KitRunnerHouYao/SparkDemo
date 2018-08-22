import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.apache.log4j.PropertyConfigurator;


import org.apache.log4j.Logger;

public class CreateTables {

	private static String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	private static Logger log = Logger.getLogger(CreateTables.class);

	
	public static void createBOND_BSC_INFO(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_BSC_INFO"
					+ "(deal_dt            DATE not NULL ," +
					"  bond_id            VARCHAR(40) not NULL ," +
					"  bond_cd            VARCHAR(90)," +
					"  bond_nm            VARCHAR(300)," +
					"  bond_ctgry_nm      VARCHAR(100)," +
					"  yr_of_time_to_mrty DECIMAL," +
					"  frst_value_dt      DATE," +
					"  mrty_dt            DATE," +
					"  bond_prd           VARCHAR(300)," +
					"  issue_vlmn         DECIMAL(38,10)," +
					"  cstdn              VARCHAR(100)," +
					"  trd_amnt           DECIMAL(38,10)," +
					"  deal_cnt           DECIMAL," +
					"  day_turnover_rate  DECIMAL(38,10)," +
					"CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id))");
			System.out.println("create BOND_BSC_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_TURNOVER_RATE(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_TURNOVER_RATE"
					+ "(deal_dt           DATE NOT NULL ," +
					"  bond_id           VARCHAR(40) NOT NULL ," +
					"  bond_prd          VARCHAR(512) NOT NULL ," +
					"  bond_ctgry_nm2    VARCHAR(100) NOT NULL ," +
					"  bond_cd           VARCHAR(90)," +
					"  bond_nm           VARCHAR(300)," +
					"  day_turnover_rate DECIMAL(38,10)," +
					"  trd_amnt_sum      DECIMAL(38,10)," +
					"  zy_rate           DECIMAL(38,10)," +
					"CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id,bond_prd,bond_ctgry_nm2))");
			System.out.println("create BOND_TURNOVER_RATE succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_INCOME(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_INCOME"
					+ "(deal_dt        DATE not NULL ," +
					"  bond_id        VARCHAR(40) not null," +
					"  org_id         VARCHAR(40) NOT NULL ," +
					"  bond_cd        VARCHAR(90)," +
					"  bond_nm        VARCHAR(1000)," +
					"  buy_paper_all  DECIMAL," +
					"  buy_act_all    DECIMAL," +
					"  sell_paper_all DECIMAL," +
					"  sell_act_all   DECIMAL," +
					"CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id,org_id))");
			System.out.println("create BOND_INCOME succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_DEAL_TREND(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_DEAL_TREND"
					+ "(deal_dt           DATE not NULL ," +
					"  bond_id            VARCHAR(40) not NULL ," +
					"  bond_cd           VARCHAR(90)," +
					"  bond_nm           VARCHAR(1000)," +
					"  dirty_price_wg_bf DECIMAL," +
					"  dirty_price_wg_af DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id))");
			System.out.println("create BOND_DEAL_TREND succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_DEAL_TREND_ORG(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_DEAL_TREND_ORG"
					+ "(deal_dt           DATE not  null," +
					"  bond_id            VARCHAR(40) not NULL ," +
					"  org_id            VARCHAR(40) NOT NULL ," +
					"  bond_cd           VARCHAR(90)," +
					"  bond_nm           VARCHAR(1000)," +
					"  ins_show_name     VARCHAR(300) ," +
					"  buy_ytm_rate_bf   DECIMAL," +
					"  buy_ytm_rate_af   DECIMAL," +
					"  sell_ytm_rate_bf  DECIMAL," +
					"  sell_ytm_rate_af  DECIMAL,"
					+ "CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id,org_id))");
			System.out.println("create BOND_DEAL_TREND_ORG succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_DEAL_ORGTYPE(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_DEAL_ORGTYPE"
					+ "(deal_dt       DATE not NULL ," +
					"  bond_id       VARCHAR(40) not null," +
					"  bond_cd       VARCHAR(90)," +
					"  bond_nm       VARCHAR(1000)," +
					"  ins_show_name VARCHAR(300) not null," +
					"  trd_amnt      DECIMAL," +
					"  net_amnt      DECIMAL," +
					"CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id,ins_show_name))");
			System.out.println("create BOND_DEAL_ORGTYPE succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_DEAL_ORGLIST(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_DEAL_ORGLIST"
					+ "(deal_dt       DATE not NULL ," +
					"  bond_id       VARCHAR(40) not null," +
					"  bond_ctgry_id VARCHAR(40) not NULL," +
					"  org_id        VARCHAR(40)not NULL," +
					"  ins_show_name VARCHAR(300) not NULL," +
					"  bond_cd       VARCHAR(90) ," +
					"  bond_nm       VARCHAR(1000)," +
					"CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id,bond_ctgry_id,org_id,ins_show_name))");
			System.out.println("create BOND_DEAL_ORGLIST succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createBOND_DEAL_SMART(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS BOND_DEAL_SMART"
					+ "(deal_dt        DATE NOT  NULL ," +
					"  bond_id        VARCHAR(40) not null," +
					"  bond_cd        VARCHAR(90)," +
					"  bond_nm        VARCHAR(300)," +
					"  bond_ctgry_nm2 VARCHAR(100)," +
					"  trade_normal   DECIMAL," +
					"  trade_smart    DECIMAL," +
					"CONSTRAINT my_pk PRIMARY KEY (deal_dt,bond_id))");
			System.out.println("create BOND_DEAL_SMART succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void createXBOND_INFO(Connection conn) {
		try {
			PreparedStatement pstmt;
			pstmt = conn.prepareStatement("create table IF NOT EXISTS XBOND_INFO" +
					"(" +
					"  bond_cd VARCHAR(90) NOT NULL PRIMARY KEY," +
					"  bond_nm VARCHAR(300)" +
					")");
			System.out.println("create XBOND_INFO succeed!");
			pstmt.execute();
			pstmt.close();
		}catch (Exception e) {
			e.printStackTrace();

		}
	}

	public static void dropTable(Connection conn,String tableName){
		PreparedStatement pstmt;
		String dropString  = "Drop table "+tableName;
		try{
			pstmt = conn.prepareStatement(dropString);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void dropIndex(Connection conn,String tableName,String indexName){
		PreparedStatement pstmt;
		String dropString  = "Drop index "+indexName +" on "+tableName;
		try{
			pstmt = conn.prepareStatement(dropString);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void createIndex4BOND_BSC_INFO(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_BSC_INFO_index on BOND_BSC_INFO (" +
					"DEAL_DT,BOND_ID,BOND_CD,BOND_NM,BOND_CTGRY_NM)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void createIndex4BOND_TURNOVER_RATE(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_TURNOVER_RATE_index on BOND_TURNOVER_RATE (" +
					"DEAL_DT,BOND_ID,BOND_CD,BOND_PRD,BOND_CTGRY_NM2)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void createIndex4BOND_DEAL_TREND(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_DEAL_TREND_index on BOND_DEAL_TREND (" +
					"DEAL_DT,BOND_ID,BOND_CD,BOND_NM)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	public static void createIndex4BOND_DEAL_TREND_ORG(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_DEAL_TREND_ORG_index on BOND_DEAL_TREND_ORG (" +
					"DEAL_DT,BOND_ID,BOND_CD,ORG_ID,INS_SHOW_NAME,BOND_NM)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void createIndex4BOND_DEAL_ORGTYPE(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_DEAL_ORGTYPE_index on BOND_DEAL_ORGTYPE (" +
					"DEAL_DT,BOND_ID,BOND_CD,INS_SHOW_NAME,BOND_NM)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void createIndex4BOND_DEAL_ORGLIST(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_DEAL_ORGLIST_index on BOND_DEAL_ORGLIST (" +
					"DEAL_DT,BOND_ID,BOND_CD,INS_SHOW_NAME,BOND_NM,ORG_ID,BOND_CTGRY_ID)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	public static void createIndex4BOND_INCOME(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_INCOME_index on BOND_INCOME (" +
					"DEAL_DT,BOND_ID,BOND_CD,ORG_ID,BOND_NM)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void createIndex4BOND_DEAL_SMART(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_BOND_DEAL_SMART_index on BOND_DEAL_SMART (" +
					"DEAL_DT,BOND_ID,BOND_CD,BOND_NM,BOND_CTGRY_NM2)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	public static void createIndex4XBOND_INFO(Connection conn){
		try{
			PreparedStatement pstmt;
			String index1 = "create local index IF NOT EXISTS GZ4_XBOND_INFO_index on XBOND_INFO (" +
					"BOND_CD,BOND_NM)";
			pstmt = conn.prepareStatement(index1);
			pstmt.execute();
			pstmt.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}


	public static void createTables(Connection conn) {

		/*
				个债表 崇高
		 */
		// 个债基本信息，个债交易信息  个债活跃度分析/活跃度分析图例
		createBOND_BSC_INFO(conn);
		//个债活跃度分析/个债换手率走势图  保存 每日换手率
		createBOND_TURNOVER_RATE(conn);
		//个债整体表现
		createBOND_INCOME(conn);
		//个债成交价分析/成交价走势图
		createBOND_DEAL_TREND(conn);
		createBOND_DEAL_TREND_ORG(conn);
		//个债交易对手分析 分机构类型成交量分布图 分机构类型净买入(卖出)债券分布图
		createBOND_DEAL_ORGTYPE(conn);
		//个债交易对手分析 分机构类型买卖债券机构数量分布 机构需去重复处理 提供每日机构明细列表
		createBOND_DEAL_ORGLIST(conn);
		//个债策略交易分析
		createBOND_DEAL_SMART(conn);
		//xbond_info
		createXBOND_INFO(conn);
		
	}

	public static void createIndexes(Connection conn){
		

		createIndex4BOND_BSC_INFO(conn);
		createIndex4BOND_TURNOVER_RATE(conn);
		createIndex4BOND_INCOME(conn);
		createIndex4BOND_DEAL_TREND(conn);
		createIndex4BOND_DEAL_TREND_ORG(conn);
		createIndex4BOND_DEAL_ORGTYPE(conn);
		createIndex4BOND_DEAL_ORGLIST(conn);
		createIndex4BOND_DEAL_SMART(conn);
		createIndex4XBOND_INFO(conn);
		System.out.println("LOCAL INDEX FOR ge-zhai TABLES succeed!");

	}

	public static  void dropTables(Connection conn){
		String []sqls = new String[9];
		
		//崇高 个债涉及的 9张表

		sqls[0] = "Drop table IF Exists BOND_BSC_INFO";
		sqls[1] = "Drop table IF Exists BOND_TURNOVER_RATE";
		sqls[2] = "Drop table IF Exists BOND_INCOME";
		sqls[3] = "Drop table IF Exists BOND_DEAL_TREND";
		sqls[4] = "Drop table IF EXISTS BOND_DEAL_TREND_ORG";
		sqls[5] = "Drop table IF Exists BOND_DEAL_ORGTYPE";
		sqls[6] = "Drop table IF Exists BOND_DEAL_ORGLIST";
		sqls[7] = "Drop table IF Exists BOND_DEAL_SMART";
		sqls[8] = "Drop table IF Exists XBOND_INFO";

		try{
			Statement pstmt = conn.createStatement();
			for(int i=0;i<sqls.length;i++) {
				pstmt.addBatch(sqls[i]);
			}
			pstmt.executeBatch();
			conn.commit();
			pstmt.close();
			System.out.println("drop  tables succeed!");

		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public static void dropIndexes(Connection conn){
		String []sqls = new String[9];

		sqls[0] = "Drop index IF EXISTS GZ4_BOND_BSC_INFO_index on BOND_BSC_INFO";
		sqls[1] = "Drop index IF EXISTS GZ4_BOND_TURNOVER_RATE_index on BOND_TURNOVER_RATE";
		sqls[2] = "Drop index IF EXISTS  GZ4_BOND_INCOME_index on BOND_INCOME ";
		sqls[3] = "Drop index IF EXISTS  GZ4_BOND_DEAL_TREND_index on BOND_DEAL_TREND";
		sqls[4] = "Drop index IF EXISTS  GZ4_BOND_DEAL_TREND_ORG_index on BOND_DEAL_TREND_ORG";
		sqls[5] = "Drop index IF EXISTS GZ4_BOND_DEAL_ORGTYPE_index on BOND_DEAL_ORGTYPE";
		sqls[6] = "Drop index IF EXISTS GZ4_BOND_DEAL_ORGLIST_index on BOND_DEAL_ORGLIST ";
		sqls[7] = "Drop index IF EXISTS GZ4_BOND_DEAL_SMART_index on BOND_DEAL_SMART";
		sqls[8] = "Drop index IF EXISTS GZ4_XBOND_INFO_index on XBOND_INFO ";

		try{
			Statement pstmt = conn.createStatement();
			for(int i=0;i<sqls.length;i++) {
				pstmt.addBatch(sqls[i]);
			}
			pstmt.executeBatch();
			conn.commit();
			pstmt.close();
			System.out.println("drop  indexes succeed!");

		}catch (Exception e){
			e.printStackTrace();
		}
	}

	//args:127.0.0.1:2181
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		PropertyConfigurator.configure("conf/log4j.properties");
		PropertyConfigurator.configure(ClassLoader.getSystemResource("log4j.properties"));
		// TODO Auto-generated method stub
		String CONNECTION_URL = "jdbc:phoenix:";
		if(args.length==1){
			CONNECTION_URL = CONNECTION_URL+args[0];
		}else{
			log.warn("CONNECTION_URL = jdbc:phoenix:127.0.0.1:2181");
			CONNECTION_URL = "jdbc:phoenix:127.0.0.1:2181";
		}
		Connection conn = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(CONNECTION_URL);
			//删除索引 和表,防止测试数据对结果的影响
			long b1Time  = System.currentTimeMillis();
			dropIndexes(conn);
			long b2Time  = System.currentTimeMillis();
			System.out.println("drop indexes succeed! Cost "+(b2Time-b1Time)/1000 +" seconds");

			dropTables(conn);
			long b3Time  = System.currentTimeMillis();
			System.out.println("drop tables succeed! Cost "+(b3Time-b2Time)/1000 +" seconds");

			// 创建表
			createTables(conn);
			long b4Time  = System.currentTimeMillis();
			System.out.println("create tables succeed! Cost "+(b4Time-b3Time)/1000 +" seconds");
			//创建索引
			createIndexes(conn);
			long b5Time  = System.currentTimeMillis();
			conn.close();

			System.out.println("create indexes succeed! Cost "+(b5Time-b4Time)/1000 +" seconds");
			System.out.println(" initial phase total running time is " +(b5Time-b1Time)/1000 +" seconds" );

		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
