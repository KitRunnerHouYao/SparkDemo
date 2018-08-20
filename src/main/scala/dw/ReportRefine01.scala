import java.sql.DriverManager

import org.apache.hadoop
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Set}

/**
  * Created by zzg on 2018/7/20.
  * 使用 6位码标识机构。
  */
object ReportRefine01 {

  val conf = new SparkConf().setAppName("Report").setMaster("local[2]") //！！！集群上改！！！
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.ERROR)

  val JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"
  val CONNECTION_URL = "jdbc:phoenix:127.0.0.1:2181"
  try
    Class.forName(JDBC_DRIVER)
  catch {
    case e: Exception =>
      // TODO: handle exception
      e.printStackTrace()
  }

  /*
   args[0]=pd_d_path ; args[1] market_close_type_I; args[2]=member_ctgry_d
   args[3]=member_d_path ; args[4]= bond_repo_deal_path ;
   args[5]=trdx_deal_infrmn_rmv_d_path ; args[6]= bond_d
   args[7]=ri_credit_rtng ;  args[8]=dps_v_cr_dep_txn_dtl_data
   args[9] = ev_cltrl_dtls
   */

  def main(args: Array[String]): Unit = {
    //创建 JDBC 连接

    val conn = DriverManager.getConnection(CONNECTION_URL)
    val conf = new hadoop.conf.Configuration()

    /*
   1、 清洗数据
   步骤1)。从sor.bond_repo_deal表中找出有用的质押式回购（产品定为R001,R007和R014）交易信息。
     */
    val pd_d_data = sc.textFile(args(0))
      .filter(_.length!=0).filter(!_.contains("PD")) //表名首字个缩写
      .filter(line=>{
      val lineArray = line.split(",")
      if(lineArray.length!=2){
        false
      }else{
        if(lineArray(0).length==0 ||lineArray(1).length==1){
          false
        }else{
          true
        }
      }
    })
      .map(line=>{
        val tmp =line.split(",")
        (tmp(0),tmp(1))
      })
      .filter(x=>(x._2.equals("R001") || x._2.equals("R007") || x._2.equals("R014")))
      . collectAsMap()

    //获得成员信息，得到成员可展示类型，并更改bond_repo_deal表
    //step1:得到 market_close_type_I,选出属于质押式回购的机构类型
    val market_close_type_i_data = sc.textFile(args(1))
      .filter(_.length!=0).filter(_.contains("MARKET"))
      .filter(line=>{
        val lineArray = line.split(",")
        if(lineArray.length!=3){
          false
        }else {
          for (i <- 0 until lineArray.length) {
            if (lineArray(i).length == 0) {
              false
            }
          }
          if(lineArray(0).equals("质押式回购")){
            false
          }
          true
        }
      }).map(line=>{
      val tmp = line.split(",")
      (tmp(1),tmp(2)) //只取后两个字段
    }).collectAsMap()

    val broadMARKET_CLOSE_TYPE_ID_DATA = sc.broadcast(market_close_type_i_data)

    //step1 : 将机构类型表的 类型名称映射为 可展示类型
    val member_ctgry_d_data = sc.textFile(args(2))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{
        val lineArray = line.split(",")
        if(lineArray.length != 2){
          false
        }else{
          for(i<-0 until lineArray.length){
            if(lineArray(i).length ==0){
              false
            }
          }
          true
        }
      }).map(line=>{
      val tmp = line.split(",")
      val market_close_type_data = broadMARKET_CLOSE_TYPE_ID_DATA.value
      val ins_show_name = market_close_type_data.get(tmp(1)).get//根据机构分类名得到展示端名字
      (tmp(0),ins_show_name)
    }).collectAsMap()

    val broadMEMER_CTGRY_D = sc.broadcast(member_ctgry_d_data)
    /*
    修改机构的member_d数据，增加机构类型名(可展示类型。) key(ip_id)机构ID
     */
    val member_d_rdd = sc.textFile(args(3))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{ //若某机构为父机构,则其rt_member_cd 和rt_member_full_nm 字段可能为空
        val lineArray = line.split(",")
        if(lineArray.length != 9){
          false
        }
        true
        //          else{
        //            for(i<-0 until lineArray.length){
        //              if(lineArray(i).length ==0){
        //                false
        //              }
        //            }
        //            true
        //          }
      })
      .map(line=>{
        val tmp =  line.split(",")
        val member_ctgry_d = broadMEMER_CTGRY_D.value
        val ins_show_name = member_ctgry_d.get(tmp(1)).toString
        (tmp(0), (tmp(1),tmp(2),tmp(3),tmp(4),tmp(5),tmp(6),tmp(7),tmp(8),ins_show_name)) //8是6位编码
      }).cache()

    val member_d_data = member_d_rdd.collectAsMap() //member全部信息
    val memberUniqCode2NameMap = member_d_rdd.map(record=>{
      val member_uniq = record._2._8 //成员ID
      val ins_show_name = record._2._9
      (member_uniq,ins_show_name)
    }).collectAsMap() //member展示类型名
    //对根机构，找出其member_cd->member_uniq
    val rootCd2Uniq = member_d_rdd.filter(record=>{
      val member_cd = record._2._2
      val rt_member_cd = record._2._4
      if(member_cd.equals(rt_member_cd)||rt_member_cd.isEmpty){ //若自己CD跟父的CD相同，则证明是根机构
        true
      }else{
        false
      }
    }).map(record=>(record._2._2,record._2._9)).collectAsMap() //根机构 CD->uniq_code

    val broadRootCd2Uniq = sc.broadcast(rootCd2Uniq)
    //member_cd找出其父机构uniq
    val memberUniq2RootUniq  = member_d_rdd.mapValues(x=>{ //x为values
      val root_cd = x._4 //该机构的root_cd
    val rootMemberId = broadRootCd2Uniq.value.get(root_cd).get
      rootMemberId
    }).collectAsMap()

    val broadPD_D = sc.broadcast(pd_d_data)
    val broadMEMBER_D =sc.broadcast(member_d_data)
    val broadMemberUniq2Name = sc.broadcast(memberUniqCode2NameMap)
    val broadMemberUniq2RootUniq = sc.broadcast(memberUniq2RootUniq)

    ///过滤交易维度表，以便更好的与交易记录表交易
    val trdx_deal_infrmn_rmv_d_data = sc.textFile(args(5))
      .filter(_.length!=0).filter(!_.contains("TRDX")).filter(line=> {
        val lineArray = line.split(",")
        if(lineArray.length != 4){
          false
        }else{
          for(i<-0 until lineArray.length){
            if(lineArray(i).length ==0){ //过滤撤销记录
              false
            }
          }
          //过滤数据
          if( !lineArray(2).equals("109") ||(lineArray(0).equals("2") && lineArray(1).equals("1"))){
            false
          }
        }
        true
      })
      .map(line=> {
        val tmp = line.split(",")
        "11002"+tmp(3)
      }).collect().toSet

    val broadTRDX_DEAL_SUCCED_ID = sc.broadcast(trdx_deal_infrmn_rmv_d_data)

    //根据 a,d 2个条件进行过滤, 与broadTRDx相交，得到成功记录 .b条件待会使用，将ID映射为uniq
    val bond_repo_deal_f_rdd = sc.textFile(args(4))
      .filter(_.length != 0).filter(!_.contains("BOND"))
      .filter(line=>{
        val lineArray = line.split(",")
        val member_d_values = broadMEMBER_D.value
        if(lineArray.length != 10){
          false
        }else{
          for(i<-0 until lineArray.length){
            if(lineArray(i).length ==0){
              false
            }
          }//过滤撤销记录
          if(lineArray(6).equals("2")){
            false
          }
          val succeed_trade_values = broadTRDX_DEAL_SUCCED_ID.value
          if(! succeed_trade_values.contains(lineArray(0))){
            false     ////与broadTRDx相交 删除不存在记录
          }
          val repo_pty_infor =  member_d_values.get(lineArray(1))//正回购方id
          val rvrse_repo_infor = member_d_values.get(lineArray(2))  //逆回购id
          //过滤不在发行方 有效期间内的记录
          if(lineArray(7).compareTo(repo_pty_infor.get._6) <0 || lineArray(7).compareTo(repo_pty_infor.get._7)>0 ||
            lineArray(7).compareTo(rvrse_repo_infor.get._6) <0 || lineArray(7).compareTo(repo_pty_infor.get._7)>0){
            false
          }
          true
        }
      })
      .map(line=>{     //得到正逆回购方Uniq
        val tmp = line.split(",")
        val repo_uniq = broadMEMBER_D.value.get(tmp(1).trim).get._8
        val rvrse_uniq = broadMEMBER_D.value.get(tmp(2).trim).get._8
        (tmp(0).trim, (repo_uniq,rvrse_uniq,tmp(3),tmp(4).trim,tmp(5).trim,tmp(6),tmp(7),tmp(8),tmp(9)))
      })

    /*
    1、 清洗数据       步骤2)。Bond_d中bond_ctgry_nm进行变换。
    */
    val bond_d_data = sc.textFile(args(6))
      .filter(_.length!=0).filter(!_.contains("BOND"))
      .filter(line=>{
        val lineArray = line.split(",")
        if(lineArray.length != 4){
          false
        }else {
          for (i <- 0 until lineArray.length) {
            if (lineArray(i).length == 0) { //过滤撤销记录
              false
            }
          }
          true
        }
      }).map(line=>{
      val tmp = line.split(",")
      if(tmp(1).equals("国债") || tmp(1).equals("政策性金融债")){
        (tmp(0),("利率债",tmp(2),tmp(3)))
      }else if(tmp(1).equals("同业存单")){
        (tmp(0),("同业存单",tmp(2),tmp(3)))
      }else{
        (tmp(0),("信用债",tmp(2),tmp(3)))
      }
    }).collectAsMap()
    val broadBondD = sc.broadcast(bond_d_data)

    /*
    1、 清洗数据       步骤2)。sor.ri_credit_rtng中rtng_desc仅显示"AAA","AA+"和"AA及以下"。
    */

    val ri_credit_rtng = sc.textFile(args(7))
      .filter(_.length!=0).filter(!_.contains("RI"))
      .filter(line=>{
        val lineArray = line.split(",")
        if(lineArray.length != 2){
          false
        }else {
          for (i <- 0 until lineArray.length) {
            if (lineArray(i).length == 0) { //过滤撤销记录
              false
            }
          }
          true
        }
      }).map(line=>{
      val tmp = line.split(",")
      if(tmp(1).equals("AAA")){
        (tmp(0),("AAA"))
      }else if(tmp(1).equals("AA+")||tmp(1).equals("A-1+")||tmp(1).equals("AA＋")){
        (tmp(0),"AA+")
      }else{
        (tmp(0),"AA及以下")
      }
    }).collectAsMap()
    val broadRiCreditRtng = sc.broadcast(ri_credit_rtng)

    //用于找出银银间交易
    val dps_v_cr_dep_txn_dtl_data = sc.textFile(args(8))
      .filter(_.length!=0).filter(!_.contains("DPS")).filter(line=>{
        val lineArray = line.split(",")
        if(lineArray.length != 2){
          false
        }else {
          for (i <- 0 until lineArray.length) {
            if (lineArray(i).length == 0) { //过滤撤销记录
              false
            }
          }
          true
        }
      })
      .map(line=>{
        val tmp = line.split(",")
        (tmp(0).trim,tmp(1).trim)
      }).collectAsMap()
    val broadDpsDepTxnDtlData = sc.broadcast(dps_v_cr_dep_txn_dtl_data)

    //!!!!数据清洗完毕
    /*
    算法2 对bond_repo_deal清洗后的数据进行映射得到 结果相关属性
      1.将trdng_pd_id 映射成 pd_cd(产品代码)
     2.将 trdnd_pri_id 映射成 rtng_desc

    3. 不需要ev_id和ST
    使用 机构ID，产品类型，日期 作为key
     */
    //将id替换为uniq编码,删除ST字段
    // 保持格式(pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amnt,evid)
    //给第7张表以后的数据要用（它们不区分PD_CD）
    val bond_repo_deal_table =  bond_repo_deal_f_rdd.map(
      x=>{
        val repo_pty_uniq = x._2._1
        val rvrse_repo_uniq = x._2._2
        val pd_cd= broadPD_D.value.get(x._2._3).toString //根据trdng_pd_id得到 pd_cd
        val deal_number = x._2._4
        val rtng_desc = broadRiCreditRtng.value.get(x._2._5).get
        val deal_dt = x._2._7
        val deal_repo_rate = x._2._8
        val trade_amnt = x._2._9
        (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
      }
    ).cache()

    /*过滤出产品类型在 "R001" "R007" "R014"中的交易,用于表格1-6数据的统计

     */
    val bond_repo_deal_pd_filter_table = bond_repo_deal_table.filter(record=>{
      val pd_cd = record._1
      if(pd_cd.equals("R001") || pd_cd.equals("R007") || pd_cd.equals("R014")){
        true
      }else{
        false
      }
    }).cache()

    /*
      机构 正回购 按  机构（ID），(汇总类型)，产品，日期 进行分组汇总
      这是 中间结果，这是因为父机构 需要包含子机构的结果，而且务必要保存子结构结果。
      保持这个中间结果的目的，是为计算  机构类型的结果做准备（父子机构类型可能不一致！！！）
     */
    val repo_pty_day = bond_repo_deal_pd_filter_table //按产品类型过滤后的表
      .map(x=>{Tuple2(Tuple3(x._3,x._1,x._2),Tuple2(x._7*x._8,x._8))}) //key ID，产品类型,日期
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2)).cache()

    /*
    因为只需统计到父机构 各类型的 产品信息
     */
    val root_repo_pty_day = repo_pty_day
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_uniq = record._1._1
        val root_member_uniq =  broadMemberUniq2RootUniq.value.get(member_uniq).toString //根据映射找出其父机构ID
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(root_member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2))
        .mapValues(x=>Tuple2(x._1/x._2,x._2)).cache()

    root_repo_pty_day  //将正回购结果写Hbase!!!
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2._1,result._2._2)})
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "REPO_WEIGHTED_PRICE","REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
   // 机构 逆回购 按  机构，汇总类型，产品，日期 进行分组汇总
  */
    val rvrse_repo_pty_day = bond_repo_deal_pd_filter_table //按产品类型过滤后的表
      .map(x=>{Tuple2(Tuple3(x._4,x._1,x._2),Tuple2(x._7*x._8,x._8))}) //逆回购机构 ID，产品CD,日期
      .reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2)).cache()

    /*
    因为只需统计到父机构 各类型的 产品信息
   */
    val root_rvrse_repo_pty_day = rvrse_repo_pty_day
      .flatMap(record=>{ //将机构ID映射为父机构ID
        val member_id = record._1._1
        val root_member_id =  broadMemberUniq2RootUniq.value.get(member_id).toString //根据映射找出其父机构ID
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple4 (root_member_id,"总额",record._1._2,record._1._3), record._2)
        if(member_id.equals(root_member_id)){
          arrays += Tuple2(Tuple4 (root_member_id,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_id,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_id).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_id,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      })
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2)) //将结果写HBase!!
        .mapValues(x=>Tuple2(x._1/x._2,x._2)).cache() // 结果为<sum(加权价)/sum(amount),sum(amount)>

    root_rvrse_repo_pty_day //将逆回购结果写Hbase!!!
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2._1,result._2._2)})
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "RVRSE_WEIGHTED_PRICE","RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
      正逆回购间价差表
    */
    root_repo_pty_day.join(root_rvrse_repo_pty_day) //!!!!将结果写Hbase.
      .mapValues(x=>{
      val repo = x._1
      val rvrse = x._2
      Tuple2 (rvrse._1 - repo._1, repo._2+ rvrse._2)//加权价(逆-正)相减 得到价差，amount相加得 到总amount
    })
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2._1,result._2._2)})
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "PRICE_DIFFERENCE","TOTAL_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
       机构类型(展示类型，非机构真正的类型)   正回购 统计 （对汇总结果按类型汇总!）
      */
    val ctgry_repo_pty_day = repo_pty_day.map(record=>{
      val member_uniq = record._1._1 //得到机构ID
      val member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).toString
      //key: 机构类型，汇总类型，产品，日期
      (Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
      .mapValues(x=>x._1/x._2).cache() //sum(加权价)/sum(amount) ,无需保持总量

    //结果 正回购价写表
      ctgry_repo_pty_day.map(result=>{(result._1._1,result._1._2,result._1._3,result._2)})
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
      "REPO_WEIGHTED_PRICE"),conf,Some("127.0.0.1:2181"))

    /*
       机构类型(展示类型，非机构真正的类型) 逆回购 统计 （对汇总结果按类型汇总!）
      */
    val ctgry_rvrse_repo_pty_day = rvrse_repo_pty_day.map(record=>{
      val member_uniq = record._1._1 //得到机构uiq
      val member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).toString
      //key: 机构类型，汇总类型，产品，日期
      (Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
      .mapValues(x=>x._1/x._2).cache() //sum(加权价)/sum(amount)

    //结果 逆回购价写表
      ctgry_rvrse_repo_pty_day.map(result=>{(result._1._1,result._1._2,result._1._3,result._2)})
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_WEIGHTED_PRICE"),conf,Some("127.0.0.1:2181"))

    //结果 逆-正 价差写表
    ctgry_repo_pty_day.join(ctgry_rvrse_repo_pty_day)
      .mapValues(x=>{x._2 -x._1}) //逆回购-正回购
        .map(x=>(x._1,x._2))
       .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "PRICE_DIFFERENCE"),conf,Some("127.0.0.1:2181"))

    //正回购 按类型 单条记录汇总(类型,产品,日期)
    val ctgryRepoRecords = bond_repo_deal_pd_filter_table.map(x=>{
      val repo_member_uniq = x._3
      val ctgryName = broadMemberUniq2Name.value.get(repo_member_uniq).get
      Tuple2(Tuple3(ctgryName,x._1,x._2),x._7)}).cache()

    //正回购 按类型最大 写表
    ctgryRepoRecords.reduceByKey((x,y)=>{
      if(x>y){
        x
      }else{
        y
      }
    }).map(x=>(x._1,x._2))
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_MAX_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))

    //正回购 按类型最小  写Hbase
    ctgryRepoRecords.reduceByKey((x,y)=>{
      if(x<y){
        x
      }else{
        y
      }
    }).map(x=>(x._1,x._2))
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_MIN_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))

    //逆回购 单条交易汇总
    val ctgryRvrseRepoRecords = bond_repo_deal_pd_filter_table.map(x=>{
      val repo_member_uniq = x._4
      val ctgryName = broadMemberUniq2Name.value.get(repo_member_uniq).get
      Tuple2(Tuple3(ctgryName,x._1,x._2),x._7)}).cache()

    //逆回购 类型最大 写Hbase
    ctgryRvrseRepoRecords.reduceByKey((x,y)=>{
      if(x>y){
        x
      }else{
        y
      }
    }).map(x=>(x._1,x._2))
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_MAX_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))
    //逆回购 类型最小 写Hbase
    ctgryRvrseRepoRecords.reduceByKey((x,y)=>{
      if(x<y){
        x
      }else{
        y
      }
    }).map(x=>(x._1,x._2))
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_MIN_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))


    //    (pd_cd,deal_dt,repo_pty_id,rvrse_repo_pty,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
    //      ,BigDecimal.apply(trade_amnt) )
    //全市场 每个产品 每天 加权价，
    val market_records = bond_repo_deal_pd_filter_table.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._7
      val amount = record._8
      Tuple2 (Tuple2(pd_cd,deal_dt), Tuple2(repo_rate*amount,amount))
    }).reduceByKey((x,y)=>{
      (x._1 + y._1 ,x._2+y._2)
    }).mapValues(x=>x._1/x._2)
      .map(x=>(x._1._1,x._1._2,x._2))
      .saveToPhoenix("",Seq("PD_CD","DEAL_DT",
          "MARKET_WEIGHTED_PRICE"),conf,Some("127.0.0.1:2181"))

    // 求最大和最小
    val market_repo_rates = bond_repo_deal_pd_filter_table.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._7
      Tuple2 (Tuple2(pd_cd,deal_dt), repo_rate)
    }).cache
    //全市场 最大 和最小 repo_rate 求PD_CD, DEAl_DT
    market_repo_rates.reduceByKey((x,y)=>{if(x>y)x else y})
      .map(x=>{(x._1,x._2)}) //最大
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
        "MARKET_MAX_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))

    market_repo_rates.reduceByKey((x,y)=>{if(x<y)x else y})
      .map(x=>{(x._1,x._2)}) //最小
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "MARKET_MIN_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))

    //银银间 正逆 回购 加权价，最大 和最小 repo_rate
    //首先过滤数据
    val banks_records =   bond_repo_deal_pd_filter_table
      .filter(record=>{
        val deal_number = record._5//成交编号
        val dpst_instn_trdng_indctr =  broadDpsDepTxnDtlData.value.get(deal_number).toString
        if (dpst_instn_trdng_indctr.equals("1")){
          true
        }else{false}})
      .cache()

    banks_records.map(record=>{
        val pd_cd = record._1
        val deal_dt = record._2
        val repo_rate = record._7
        val amount = record._8
        Tuple2 (Tuple2(pd_cd,deal_dt), Tuple2(repo_rate * amount,amount))
      }).reduceByKey((x,y)=>{
      (x._1 + y._1,x._2+y._2)
     }).mapValues(x=>x._1/x._2)
      .map(x=>(x._1._1,x._1._2,x._2))
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
        "BANKS_WEIGHTED_PRICE"),conf,Some("127.0.0.1:2181"))

    //统计银银间 最大和最小 先将repo_rate统计出来
    val banks_repo_rates = banks_records.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._7
      Tuple2 (Tuple2(pd_cd,deal_dt), repo_rate)
    }).cache
    //银银间最大 和最小 repo_rate 求PD_CD, DEAl_DT
    banks_repo_rates.reduceByKey((x,y)=>{if(x>y)x else y})
      .map(x=>{(x._1,x._2)}) //最大
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "BANKS_MAX_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))

    banks_repo_rates.reduceByKey((x,y)=>{if(x<y)x else y})
      .map(x=>{(x._1,x._2)}) //最小
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "BANKS_MIN_DEAL_REPO_RATE"),conf,Some("127.0.0.1:2181"))

    //得到一笔交易的bond_id,首先将 ev_cltrl_dtls的 ri_id 与bond_d的 bond_id 相join
    //1.对 ev_cltrl_dtls进行预处理得到 (ev_id,(bond_id,cnvrsn_prprtn))
    val ev_cltrl_dtls = sc.textFile(args(7))
      .filter(_.length!=0).filter(!_.contains("EV"))
      .filter(line=>{
        val lineArray = line.split(",")
        if(lineArray.length != 3){
          false
        }else {
          for (i <- 0 until lineArray.length) {
            if (lineArray(i).length == 0) { //过滤撤销记录
              false
            }
          }
          true
        }
      }).map(line=>{
      val tmp = line.split(",")
      Tuple2(tmp(1).trim,Tuple2(tmp(0).trim,tmp(2)))
    }).collectAsMap()
    val broadEvCltrlDtls = sc.broadcast(ev_cltrl_dtls)

    /*
      正逆回购 在各质押物类型上的分布 root机构 参与者类型 质押物类型，每天，amount累加
      需要统计所有产品上的信息，所以用 bond_repo_deal_table
     */

    //(pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_pty_uniq,deal_number,rtng_desc,(deal_repo_rate).(trade_amnt),ev_id )
    //正回购机构，质押物类型，每天，amount
    val repo_zhiyawuleixing_rdd = bond_repo_deal_table
      .map(record=> {
        val repo_member_uniq = record._3 //正回购方
        val deal_date = record._2
        val ev_id = record._9
        val bond_id = broadEvCltrlDtls.value.get(ev_id).get._1 ///这种写法待验证。
        val bond_ctgry_nm = broadBondD.value.get(bond_id).get._1 //得到债券分类
        Tuple2(Tuple3(repo_member_uniq, bond_ctgry_nm, deal_date), record._8)
        //机构ID，质押物类型，日期，amount
      }).cache()

    //正 root机构，参与者类型，质押物类型，每天，amount
    repo_zhiyawuleixing_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      val root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
      }else{
        arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        val member_info = broadMEMBER_D.value.get(member_uniq).get
        val member_full_name = member_info._3// 得到机构全称
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2)})
      .saveToPhoenix("MEMBER_BOND_CTGRY_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    val ctgry_repo_zhiyawuleixing_rdd = repo_zhiyawuleixing_rdd.map(record=>{
      val member_uniq = record._1._1
      val ctgry_ins_show_name = broadMemberUniq2Name.value.get(member_uniq).get
      Tuple2(Tuple3(ctgry_ins_show_name,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3, result._2)})
      .saveToPhoenix("CTGRY_BOND_CTGRY_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","DEAL_DT",
        "REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
    统计逆回购的相关信息
     */
    val rvrse_repo_zhiyawuleixing_rdd = bond_repo_deal_table.map(record=>{
      val repo_member_uniq = record._4 //逆回购方
      val deal_date = record._2
      val ev_id = record._9
      val bond_id = broadEvCltrlDtls.value.get(ev_id).get._1 ///这种写法待验证。
      val bond_ctgry_nm = broadBondD.value.get(bond_id).get._1 //
      Tuple2(Tuple3(repo_member_uniq, bond_ctgry_nm, deal_date), record._8)
      //机构ID，质押物类型，日期，amount
    }).cache()

    //统计 根机构信息,得到每个机构的在每个分布上的amout之和     结果写HBase！！！！！
    val root_rvrse_repo_zhiyawuleixing_rdd = rvrse_repo_zhiyawuleixing_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2)})
      .saveToPhoenix("MEMBER_BOND_CTGRY_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","DEAL_DT", "RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    //统计 机构类型信息,得到每个机构的在每个分布上的amout之和    结果写HBase!!!!!!
    val ctgry_rvrse_zhiyawuleixing_rdd = rvrse_repo_zhiyawuleixing_rdd
      .map(record=>{
        val member_uniq = record._1._1
        val ctgry_ins_show_name = broadMemberUniq2Name.value.get(member_uniq).get
        Tuple2(Tuple3(ctgry_ins_show_name,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3, result._2)})
      .saveToPhoenix("CTGRY_BOND_CTGRY_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","DEAL_DT",
        "RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /* 正回购
      机构在 评级上的分布 根机构机构 信用评级，每天，amount累加
    输入表结构：(pd_cd,deal_dt,repo_pty_id,rvrse_repo_pty,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
     */
    val repo_bond_credit_deal_rdd = bond_repo_deal_table
      .map(record=>{
        val repo_member_uniq = record._3
        val deal_date = record._2
        val rtng_desc = record._6
        Tuple2(Tuple3(repo_member_uniq,rtng_desc,deal_date),record._8)
        //机构ID，信用类型，日期，amount
      }).cache()

    // 根机构 评级统计 结果 写Hbase!!!!!
    repo_bond_credit_deal_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2)})
      .saveToPhoenix("MEMBER_BOND_RTNG_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "RTNG_DESC","DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))


    // 机构类型 评级统计 结果 写Hbase!!!!!
    repo_bond_credit_deal_rdd
      .map(record=>{
        val ctgry_ins_show_type = broadMemberUniq2Name.value.get(record._1._1).toString
        Tuple2(Tuple3(ctgry_ins_show_type,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3, result._2)})
      .saveToPhoenix("CTGRY_BOND_RTNG_DEAL_INFO",Seq("INS_SHOW_NAME", "RTNG_DESC","DEAL_DT",
        "REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
    逆回购 在质押物类型上的分布
     */
    val rvrse_repo_bond_credit_deal_rdd = bond_repo_deal_table
      .map(record=>{
        val repo_member_id = record._4
        val deal_date = record._2
        val rtng_desc = record._6
        Tuple2(Tuple3(repo_member_id,rtng_desc,deal_date),record._8)
        //机构ID，信用类型，日期，amount
      }).cache()

    // 根机构 评级统计 结果 写Hbase!!!!!
    rvrse_repo_bond_credit_deal_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4, result._2)})
      .saveToPhoenix("MEMBER_BOND_RTNG_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "RTNG_DESC","DEAL_DT", "RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    // 机构类型 评级统计 结果 写Hbase!!!!!
    val ctgry_rvrse_bond_credit_deal = rvrse_repo_bond_credit_deal_rdd
      .map(record=>{
        val ctgry_ins_show_type = broadMemberUniq2Name.value.get(record._1._1).toString
        Tuple2(Tuple3(ctgry_ins_show_type,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{(result._1._1,result._1._2,result._1._3, result._2)})
      .saveToPhoenix("CTGRY_BOND_RTNG_DEAL_INFO",Seq("INS_SHOW_NAME", "RTNG_DESC","DEAL_DT",
        "RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
     质押物折扣率走势图检索:债券类型检索
    输入表结构：(pd_cd,deal_dt,repo_pty_id,rvrse_repo_pty,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
 */
    val repo_bond_discount_rdd = bond_repo_deal_table.map(record=>{
      val ev_id = record._9
      val repo_uniq = record._3
      val bond_id = broadEvCltrlDtls.value.get(ev_id).get._1 ///这种写法待验证？。
      val cnvrsn_prprtn = BigDecimal.apply(broadEvCltrlDtls.value.get(ev_id).get._2)  //折扣率
      val bond_ctgry_nm = broadBondD.value.get(bond_id).get._1   //质押物类型
      val rtng_desc = record._6
      val deal_date =  record._2
      val amount  = record._8
      Tuple2(Tuple4(repo_uniq,bond_ctgry_nm,rtng_desc,deal_date),Tuple2(cnvrsn_prprtn*amount,amount))
      //key:机构ID，质押物类型，评级，日期； value：折扣率，amount
    }).cache()
    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"总额",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"自营",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4,result._1._5,
        result._2._1/result._2._2,result._2._2)})
      .saveToPhoenix("MEMBER_BOND_TYPE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","RTNG_DESC","DEAL_DT", "REPO_WEIGHTED_CNVRSN_PRPRTN","REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_rdd.map(record=>{
      val ctgry_ins_show_type = broadMemberUniq2Name.value.get(record._1._1).toString
      Tuple2(Tuple4(ctgry_ins_show_type,record._1._2,record._1._3,record._1._4),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4,result._2._1/result._2._2)})//无需写amount
      .saveToPhoenix("CTGRY_BOND_TYPE_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM",
        "RTNG_DESC","DEAL_DT", "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some("127.0.0.1:2181"))

    /*
    对逆回购进行分析
*/
    val rvrse_repo_bond_discount_rdd = bond_repo_deal_table.map(record=>{
      val ev_id = record._9
      val rvrse_repo_uniq = record._4
      val bond_id = broadEvCltrlDtls.value.get(ev_id).get._1 ///这种写法待验证？。
      val cnvrsn_prprtn = BigDecimal.apply(broadEvCltrlDtls.value.get(ev_id).get._2)  //折扣率
      val bond_ctgry_nm = broadBondD.value.get(bond_id).get._1   //质押物类型
      val rtng_desc = record._6
      val deal_date =  record._2
      val amount  = record._8
      Tuple2(Tuple4(rvrse_repo_uniq,bond_ctgry_nm,rtng_desc,deal_date),Tuple2(cnvrsn_prprtn*amount,amount))
      //key:机构ID，质押物类型，评级，日期； value：折扣率，amount
    }).cache()
    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        val root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"总额",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"自营",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4,result._1._5,
        result._2._1/result._2._2,result._2._2)})
      .saveToPhoenix("MEMBER_BOND_TYPE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","RTNG_DESC","DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN","RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_rdd.map(record=>{
      val ctgry_ins_show_type = broadMemberUniq2Name.value.get(record._1._1).toString
      Tuple2(Tuple4(ctgry_ins_show_type,record._1._2,record._1._3,record._1._4),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._1._4,result._2._1/result._2._2)})//无需写amount
      .saveToPhoenix("CTGRY_BOND_TYPE_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM",
      "RTNG_DESC","DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some("127.0.0.1:2181"))
    /*
     质押物折扣率走势图检索:发行机构检索     正回购
    输入表结构：(pd_cd,deal_dt,repo_pty_id,rvrse_repo_pty,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
 */
    val repo_bond_discount_issr_rdd = bond_repo_deal_table.map(record=>{
      val ev_id = record._9
      val repo_uniq = record._3
      val bond_id = broadEvCltrlDtls.value.get(ev_id).get._1 ///这种写法待验证？。
      val cnvrsn_prprtn =BigDecimal.apply(broadEvCltrlDtls.value.get(ev_id).get._2)
      val issur_nm = broadBondD.value.get(bond_id).get._3
      val rtng_desc = record._6
      val deal_date =  record._2
      val amount  = record._8
      Tuple2(Tuple3(repo_uniq,issur_nm,deal_date),Tuple2(cnvrsn_prprtn*amount,amount))
      //key:机构ID，质押物类型，评级，日期； value：折扣率，amount
    }).cache()

    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_issr_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      val root_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).toString
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
      arrays += Tuple2(Tuple4 (root_uniq,"总额",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_uniq)){
        arrays += Tuple2(Tuple4 (root_uniq,"自营",record._1._2,record._1._3), record._2)
      }else{
        arrays += Tuple2(Tuple4 (root_uniq,"产品",record._1._2,record._1._3), record._2)
        val member_info = broadMEMBER_D.value.get(member_uniq).get
        val member_full_name = member_info._3// 得到机构全称
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{(record._1._1,record._1._2,record._1._3,record._1._4,record._2._1/record._2._2,record._2._2)})
      .saveToPhoenix("MEMBER_BOND_ISSR_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "ISSR_NM","DEAL_DT","REPO_WEIGHTED_CNVRSN_PRPRTN","REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))


    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_issr_rdd.map(record=>{
      val ctgry_ins_show_type = broadMemberUniq2Name.value.get(record._1._1).toString
      Tuple2(Tuple3(ctgry_ins_show_type,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._2._1/result._2._2)})//无需写amount
      .saveToPhoenix("CTGRY_BOND_ISSR_DEAL_INFO",Seq("INS_SHOW_NAME", "ISSR_NM","DEAL_DT",
      "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some("127.0.0.1:2181"))

    val rvrse_repo_bond_discount_issr_rdd = bond_repo_deal_table.map(record=>{
      val ev_id = record._9
      val rvrse_repo_uniq = record._4
      val bond_id = broadEvCltrlDtls.value.get(ev_id).get._1 ///这种写法待验证？。
      val cnvrsn_prprtn =BigDecimal.apply(broadEvCltrlDtls.value.get(ev_id).get._2)
      val issur_nm = broadBondD.value.get(bond_id).get._3
      val rtng_desc = record._6
      val deal_date =  record._2
      val amount  = record._8
      Tuple2(Tuple3(rvrse_repo_uniq,issur_nm,deal_date),Tuple2(cnvrsn_prprtn*amount,amount))
      //key:机构ID，质押物类型，评级，日期； value：折扣率，amount
    })
    ///按根机构 进行统计 结果写Hbase
    rvrse_repo_bond_discount_issr_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      val root_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).toString
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
      arrays += Tuple2(Tuple4 (root_uniq,"总额",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_uniq)){
        arrays += Tuple2(Tuple4 (root_uniq,"自营",record._1._2,record._1._3), record._2)
      }else{
        arrays += Tuple2(Tuple4 (root_uniq,"产品",record._1._2,record._1._3), record._2)
        val member_info = broadMEMBER_D.value.get(member_uniq).get
        val member_full_name = member_info._3// 得到机构全称
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{(record._1._1,record._1._2,record._1._3,record._1._4,record._2._1/record._2._2,record._2._2)})
      .saveToPhoenix("MEMBER_BOND_ISSR_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE", "ISSR_NM",
        "DEAL_DT","RVRSE_WEIGHTED_CNVRSN_PRPRTN","RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))


    //按机构类型 进行统计 结果写Hbase
   rvrse_repo_bond_discount_issr_rdd.map(record=>{
      val member_uniq = record._1._1
      val ctgry_ins_show_type = broadMemberUniq2Name.value.get(member_uniq).toString
      Tuple2(Tuple3(ctgry_ins_show_type,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)})
      .map(result=>{(result._1._1,result._1._2,result._1._3,result._2._1/result._2._2)})//无需写amount
      .saveToPhoenix("CTGRY_BOND_ISSR_DEAL_INFO",Seq("INS_SHOW_NAME", "ISSR_NM",
     "DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some("127.0.0.1:2181"))

    /*
    机构交易 分布图例
     原始输入数据 (pd_cd,deal_dt,repo_pty_id,rvrse_repo_pty,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
     结果 输出数据。（根）机构ID，参与者类型,对比机构类型,日期,交易金额
     */
    //考虑正回购信息
    val repo_pty_distribution_day = bond_repo_deal_table
      .map(x=>{
        val member_uniq = x._3
        //  val rootMemberID  = broadMemberID2RootMemberID.value.get(memberID).get
        val rvrMember_uniq = x._4
        val rvrCtgryName = broadMemberUniq2Name.value.get(rvrMember_uniq).get
        val dealDate = x._2
        Tuple2(Tuple3(member_uniq,rvrCtgryName,dealDate),x._8)
      }).cache() //key ID，产品类型,日期;VALUE:量

    //正回购 按机构进行统计，结果写Hbase!（注意：由于结果不分 正逆，因此 需要跟 逆的结果进行合并）
    repo_pty_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        val rootMemberUniq  = broadMemberUniq2RootUniq.value.get(member_uniq).get
        Tuple2(Tuple3(rootMemberUniq,record._1._2,record._1._3),record._2)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (rootMemberUniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(rootMemberUniq)){
          arrays += Tuple2(Tuple4 (rootMemberUniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (rootMemberUniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (rootMemberUniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x+y)) //key: ID,参与者类型,对比机构类型,日期 value:amount
        .map(record=>{(record._1._1,record._1._2,record._1._3,record._1._4,record._2)})
        .saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))


    //正回购 按机构类型统计，结果写Hbase! （注意：由于结果不分 正逆，因此 需要跟 逆的结果进行合并）
    repo_pty_distribution_day
      .map(record=>{
        val member_uniq = record._1._1
        val ctgryName  = broadMemberUniq2Name.value.get(member_uniq).get
        Tuple2(Tuple3(ctgryName,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>(x+y)) //
      .map(record=>{(record._1._1,record._1._2,record._1._3,record._2)})
      .saveToPhoenix("CTGRY_CTGRY_AMOUNT_DEAL_INFO",Seq("INS_SHOW_NAME",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))


    //考虑逆回购 信息
    val rvrse_repo_pty_distribution_day = bond_repo_deal_table
      .map(x=>{
        val rvrMemberUniq = x._4
        //  val rootMemberID  = broadMemberID2RootMemberID.value.get(memberID).get
        val repoMemberUniq = x._3
        val repoCtgryName = broadMemberUniq2Name.value.get(repoMemberUniq).get
        val dealDate = x._2
        Tuple2(Tuple3(rvrMemberUniq,repoCtgryName,dealDate),x._8)
      }).cache() //key ID，产品类型,日期

    //逆回购 按机构统计
    rvrse_repo_pty_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        val rootMemberUniq  = broadMemberUniq2RootUniq.value.get(member_uniq).get
        Tuple2(Tuple3(rootMemberUniq,record._1._2,record._1._3),record._2)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (rootMemberUniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(rootMemberUniq)){
          arrays += Tuple2(Tuple4 (rootMemberUniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (rootMemberUniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (rootMemberUniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x+y)) //key: ID,参与者类型,对比机构类型,日期 value:amount
      .map(record=>{(record._1._1,record._1._2,record._1._3,record._1._4,record._2)})
      .saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))


    //逆回购 按机构类型统计，结果写Hbase!
    rvrse_repo_pty_distribution_day
      .map(record=>{
        val member_uniq = record._1._1
        val ctgryName  = broadMemberUniq2Name.value.get(member_uniq).get
        Tuple2(Tuple3(ctgryName,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>(x+y))
      .map(record=>{(record._1._1,record._1._2,record._1._3,record._2)})
      .saveToPhoenix("CTGRY_CTGRY_AMOUNT_DEAL_INFO",Seq("INS_SHOW_NAME",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_SUM_AMOUNT"),conf,Some("127.0.0.1:2181"))

    /*
        分布图 考虑在 各机构类型（机构数）上的分布
     */
    //考虑 机构 正回购信息
    val repo_pty_jigou_distribution_day = bond_repo_deal_table
      .map(x=>{
        val member_uniq = x._3
        //  val rootMemberID  = broadMemberID2RootMemberID.value.get(memberID).get
        val rvrMemberUniq = x._4
        val rvrCtgryName = broadMemberUniq2Name.value.get(rvrMemberUniq).get
        val dealDate = x._2
        val rvrSet = Set(rvrMemberUniq)
        Tuple2(Tuple3(member_uniq,rvrCtgryName,dealDate),rvrSet)
      }).cache() //key ID，产品类型,日期

    repo_pty_jigou_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        val rootMemberUniq =  broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Set[String])]()
        arrays += Tuple2(Tuple4 (rootMemberUniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(rootMemberUniq)){
          arrays += Tuple2(Tuple4(rootMemberUniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (rootMemberUniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (rootMemberUniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>{x++y}) //得到 每个机构的结果集
      .map(record=>{
        val member_uniq =  record._1._1
        val ins_show_type = broadMemberUniq2Name.value.get(member_uniq).get
        val memberSetString = record._2.mkString(",")
        (ins_show_type,member_uniq,record._1._2,record._1._3,record._1._4, memberSetString)
    }).saveToPhoenix("MEMBER_CTGRY_NUMBER_DEAL_INFO",Seq("INS_SHOW_NAME","UNQ_ID_INS_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_COU_PARTY_UNIQ_SET"),conf,Some("127.0.0.1:2181"))

    //考虑 机构 逆回购信息
    val rvrse_repo_pty_jigou_distribution_day = bond_repo_deal_table
      .map(x=>{
        val repoMemberUniq = x._3
        //  val rootMemberID  = broadMemberID2RootMemberID.value.get(memberID).get
        val rvrMemberUniq = x._4
        val repoCtgryName = broadMemberUniq2Name.value.get(repoMemberUniq).get
        val dealDate = x._2
        val repoSet = Set(rvrMemberUniq)
        Tuple2(Tuple3(rvrMemberUniq,repoCtgryName,dealDate),repoSet)
      }).cache()//key ID，产品类型,日期

    //逆回购 对手机构集合统计
    rvrse_repo_pty_jigou_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        val rootMemberUniq =  broadMemberUniq2RootUniq.value.get(member_uniq).get
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Set[String])]()
        arrays += Tuple2(Tuple4 (rootMemberUniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(rootMemberUniq)){
          arrays += Tuple2(Tuple4(rootMemberUniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (rootMemberUniq,"产品",record._1._2,record._1._3), record._2)
          val member_info = broadMEMBER_D.value.get(member_uniq).get
          val member_full_name = member_info._3// 得到机构全称
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (rootMemberUniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>{x++y}) //得到 每个机构的结果集
      .map(record=>{
      val member_uniq =  record._1._1
      val ins_show_type = broadMemberUniq2Name.value.get(member_uniq).get
      val memberSetString = record._2.mkString(",")
      (ins_show_type,member_uniq,record._1._2,record._1._3,record._1._4, memberSetString)
    }).saveToPhoenix("MEMBER_CTGRY_NUMBER_DEAL_INFO",Seq("INS_SHOW_NAME","UNQ_ID_INS_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_COU_PARTY_UNIQ_SET"),conf,Some("127.0.0.1:2181"))
  }
}
