import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set
import org.apache.phoenix.spark._
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration

/**
  * Created by zzg on 2018/7/20.
  * 使用 6位码标识机构。
  * 修改了如下两个Bug:1.scala BigDecimal 不能直接写phoenix.这是因为phoenix Decimal 映射成java.math.Decimal
  * 2.日期转换的问题。只需转换成 util.date即可
  * 3.改变CSV文件提取方式,去掉引号
  * 4.针对 broadcast对象中不存在所需对象值作出修改
  * 5.使用日志:根据键值对找不到对应value的，统一设为ERROR
  * 6.前期未考虑sor.ev_cltrl_dtls表较大问题，现改为使用两表join处理方式
  */

object ReportRefine {

  val sparkconf = new SparkConf().setAppName("Report").setMaster("local[2]") //！！！集群上改！！！
  val sc = new SparkContext(sparkconf)
  @transient lazy val  log = Logger.getLogger(this.getClass)

  val ZOOKEEPER_URL="127.0.0.1:2181"


  /*
   args[0]=pd_d_path ; args[1] market_close_type_I; args[2]=member_ctgry_d
   args[3]=member_d_path ; args[4]= bond_repo_deal_path ;
   args[5]=trdx_deal_infrmn_rmv_d_path ; args[6]= bond_d
   args[7]=ri_credit_rtng ;  args[8]=dps_v_cr_dep_txn_dtl_data
   args[9] = ev_cltrl_dtls
   */

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()

    /*
   1、 清洗数据
   步骤1)。从sor.bond_repo_deal表中找出有用的质押式回购（产品定为R001,R007和R014）交易信息。
     */

    val pd_d_data = sc.textFile(args(0))
      .filter(_.length!=0).filter(!_.contains("PD")) //表名首字个缩写
      .filter(line=>{
      val lineArray = Utils.splite(line)
      if(lineArray.size() !=2){
        false
      }else{
        if(lineArray.get(0).length==0 ||lineArray.get(1).length==0){
          false
        }else{
          true
        }
      }
    })
      .map(line=>{
        val tmp =Utils.splite(line)
        (tmp.get(0).trim,tmp.get(1).trim)
      })
   //   .filter(x=> x._2.equals("R001")|| x._2.equals("R007") || x._2.equals("R014"))//不要过滤
      . collectAsMap()
    println("file 1 PD_D read succeed! read "+pd_d_data.size+" records")
    //获得成员信息，得到成员可展示类型，并更改bond_repo_deal表
    //step1:得到 market_close_type_I,选出属于质押式回购的机构类型
    val market_close_type_i_data = sc.textFile(args(1))
      .filter(_.length!=0).filter(!_.contains("MARKET"))
      .filter(line=>{
        val lineList = Utils.splite(line)
        if(lineList.size()!=3){
          false
        }else {
            var flag = true
            for(i<-0 until lineList.size() if flag){
              if(lineList.get(i)==null || lineList.get(i).isEmpty){
                  flag =false
              }
            }
          if(!lineList.get(0).equals("质押式回购")){
            flag =false
          }
          flag
        }
      }).map(line=>{
      val lineList = Utils.splite(line)
      (lineList.get(1).trim,lineList.get(2).trim)//只取后两个字段
    }).collectAsMap()
    println("file 2 market_close_type_i read succeed!  read "+market_close_type_i_data.size+"records")

    val broadMARKET_CLOSE_TYPE_ID_DATA = sc.broadcast(market_close_type_i_data)

    //step1 : 将机构类型表的 类型名称映射为 可展示类型
    val mem_cannot_find_showname = sc.longAccumulator("mem_cannot_find_showname")
    val member_ctgry_d_data = sc.textFile(args(2))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{
        val lineList = Utils.splite(line)
        if(lineList.size() != 2){
          false
        }else{
          var flag = true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          flag
        }
      }).map(line=>{
      val lineList = Utils.splite(line)
      val market_close_type_data = broadMARKET_CLOSE_TYPE_ID_DATA.value
      var ins_show_name = "None"
      if(!market_close_type_data.contains(lineList.get(1))){
        log.error("market_close_type_data does not contain ins_type "+lineList.get(1))
        mem_cannot_find_showname.add(1l)
      }else{
        ins_show_name = market_close_type_data.get(lineList.get(1)).get//根据机构分类名得到展示端名字
      }
    //  val ins_show_name = market_close_type_data.get(lineList.get(1)).get
      (lineList.get(0),ins_show_name)
    }).collectAsMap()
    println("file 3 member_ctgry_d_data read succeed! read "+member_ctgry_d_data.size+" records")

    val broadMEMER_CTGRY_D = sc.broadcast(member_ctgry_d_data)
    /*
    修改机构的member_d数据，增加机构类型名(可展示类型。) key(ip_id)机构ID
     */
    val mem_cannot_find_ctgryID = sc.longAccumulator("mem_cannot_find_ctgryID")
    val member_d_rdd = sc.textFile(args(3))
      .filter(_.length!=0).filter(!_.contains("MEMBER")).filter(line=>{ //若某机构为父机构,则其rt_member_cd 和rt_member_full_nm 字段可能为空
      val lineList = Utils.splite(line)
        if(lineList.size() != 9){false}
        else{
          var flag = true
          for(i<-0 until lineList.size() if flag){
            if( (lineList.get(i)==null || lineList.get(i).isEmpty) && (i!=4||i!=5) ){flag =false}
          }
          flag
        }
      })
      .map(line=>{
        val lineList = Utils.splite(line)
        val member_ctgry_d = broadMEMER_CTGRY_D.value
        var ins_show_name = "None"
        if(!member_ctgry_d.contains(lineList.get(1))){
          log.error("member_ctgry_d doesnot contain member_ctgry_nm "+lineList.get(1))
          mem_cannot_find_ctgryID.add(1l)
        }else{
          ins_show_name = member_ctgry_d.get(lineList.get(1)).get
        }
        (lineList.get(0),(lineList.get(1),lineList.get(2),lineList.get(3),lineList.get(4),lineList.get(5),
        lineList.get(6),lineList.get(7),lineList.get(8),ins_show_name)) //追加 展示名称
       // (tmp(0), (tmp(1),tmp(2),tmp(3),tmp(4),tmp(5),tmp(6),tmp(7),tmp(8),ins_show_name))
      }).cache()
    println("file 4 member_d read succeed! ")

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
    println("the number of root members is: "+rootCd2Uniq.size)

    val broadRootCd2Uniq = sc.broadcast(rootCd2Uniq)
    //member_cd找出其父机构uniq
    val rootcd_cannot_find = sc.longAccumulator("rootcd_cannot_find")
    val memberUniq2RootUniq  = member_d_rdd.mapValues(x=>{ //x为values
       val root_cd = x._4 //该机构的root_cd
       var rootMemberId = "None"
       if(!broadRootCd2Uniq.value.contains(root_cd)){
         log.error("broadRootCd2Uniq doesnot contain rt_member_cd "+root_cd)
         rootcd_cannot_find.add(1l)
       }else{
         rootMemberId = broadRootCd2Uniq.value.get(root_cd).get
       }
       rootMemberId
    }).collectAsMap()

    val broadPD_D = sc.broadcast(pd_d_data)
    val broadMEMBER_D =sc.broadcast(member_d_data)
    val broadMemberUniq2Name = sc.broadcast(memberUniqCode2NameMap)
    val broadMemberUniq2RootUniq = sc.broadcast(memberUniq2RootUniq)

    ///过滤交易维度表，以便更好的与交易记录表交易
    val trdx_deal_infrmn_rmv_d_data = sc.textFile(args(5))
      .filter(_.length!=0).filter(!_.contains("TRDX")).filter(line=> {
         val lineList = Utils.splite(line)
        if(lineList.size() != 4){
          false
        }else{
          var flag  = true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          if(flag){//过滤数据
            if( (lineList.get(0).equals(2)&& !lineList.get(1).equals("1") && lineList.get(2).equals("109")) ){
              flag = false
            }
          }
          flag
        }
      })
      .map(line=> {
        val tmp = line.split(",")
        "11002"+tmp(3)
      }).collect().toSet
    println("file 6 trdx_deal_infrmn_rmv_d_data read succeed! read "+ trdx_deal_infrmn_rmv_d_data.size+" records")

    val broadTRDX_DEAL_SUCCED_ID = sc.broadcast(trdx_deal_infrmn_rmv_d_data)

    //根据 a,d 2个条件进行过滤, 与broadTRDx相交，得到成功记录 .b条件待会使用，将ID映射为uniq
    val deal_cannot_find_ip_id = sc.longAccumulator("deal_cannot_find_ip_id") //无法从member_d表中找到机构id
    val bond_repo_deal_f_rdd = sc.textFile(args(4))
      .filter(_.length != 0).filter(!_.contains("EV"))
      .filter(line=>{
        val lineList = Utils.splite(line)
        val member_d_values = broadMEMBER_D.value
        if(lineList.size() != 10){
          false
        }else{
          var flag =true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          if(flag && lineList.get(6).equals("2")) {flag= false}  //过滤撤销记录
          if(flag){
            val succeed_trade_values = broadTRDX_DEAL_SUCCED_ID.value
            if(! succeed_trade_values.contains(lineList.get(0))) {
              log.error("trdx_deal_infrmn_rmr doesnot contain EV_ID "+lineList.get(0))
              flag = false ////与broadTRDx相交 删除不存在记录
            }
          }
          if(flag){           //过滤不在发行方 有效期间内的记录
            if(!member_d_values.contains(lineList.get(1)) || !member_d_values.contains(lineList.get(2))){
               deal_cannot_find_ip_id.add(1l)
              log.error("member_d table does not contain repoID:"+lineList.get(1)+ "or rvrse id:"+lineList.get(2)+"!")
               //println("ERROR! member_d table does not contain repoID:"+lineList.get(1)+ "or rvrse id:"+lineList.get(2)+"!")
              flag=false
            }else{
              val repo_pty_infor =  member_d_values.get(lineList.get(1))//正回购方id
              val rvrse_repo_infor = member_d_values.get(lineList.get(2))  //逆回购id
              val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
              val deal_date = oracleDateFormat.parse(lineList.get(7))
              val repo_bdate = oracleDateFormat.parse(repo_pty_infor.get._6)
              val repo_edate = oracleDateFormat.parse( repo_pty_infor.get._7)
              val rvrse_bdate = oracleDateFormat.parse( rvrse_repo_infor.get._6)
              val rvrse_edate = oracleDateFormat.parse(rvrse_repo_infor.get._7)
              if(deal_date.before(repo_bdate)|| deal_date.after(repo_edate) || deal_date.before(rvrse_bdate)|| deal_date.after(rvrse_edate)){
                flag =false
              }
            }
          }
          flag
        }
      })
      .map(line=>{     //得到正逆回购方Uniq
        val lineList = Utils.splite(line)
        val repo_id = lineList.get(1).trim
        val rvrse_id = lineList.get(2).trim
        var repo_uniq = "None"
        var rvrse_uniq = "None"
        if(!broadMEMBER_D.value.contains(repo_id)){
          log.error("member_d doesnot contain member_id:"+repo_id)
        }else{
          repo_uniq = broadMEMBER_D.value.get(repo_id).get._8
        }
        if(!broadMEMBER_D.value.contains(rvrse_id)){
          log.error("member_d doesnot contain member_id:"+rvrse_id)
        }else{
          rvrse_uniq = broadMEMBER_D.value.get(rvrse_id).get._8
        }
        (lineList.get(0).trim, (repo_uniq,rvrse_uniq,lineList.get(3).trim,lineList.get(4).trim,lineList.get(5).trim,
          lineList.get(6).trim,lineList.get(7).trim,lineList.get(8).trim,lineList.get(9).trim))
      }).cache()

    println("file 5 bond_repo_deal_f read succeed! ")

    /*
    1、 清洗数据       步骤2)。Bond_d中bond_ctgry_nm进行变换。
    */
    val bond_d_data = sc.textFile(args(6))
      .filter(_.length!=0).filter(!_.contains("BOND"))
      .filter(line=>{
        val lineList = Utils.splite(line)
        if(lineList.size() != 4){
          false
        }else {
          var flag =true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          flag
        }
      }).map(line=>{
      val lineList = Utils.splite(line)
      if(lineList.get(1).equals("国债") || lineList.get(1).equals("政策性金融债")){
        (lineList.get(0),("利率债",lineList.get(2),lineList.get(3)))
      }else if(lineList.get(1).equals("同业存单")){
        (lineList.get(0),("同业存单",lineList.get(2),lineList.get(3)))
      }else{
        (lineList.get(0),("信用债",lineList.get(2),lineList.get(3)))
      }
    }).collectAsMap()
    val broadBondD = sc.broadcast(bond_d_data)
    println("file 7 bond_d read succeed! read "+ bond_d_data.size+" records")
    /*
    1、 清洗数据       步骤2)。sor.ri_credit_rtng中rtng_desc仅显示"AAA","AA+"和"AA及以下"。
    */

    val ri_credit_rtng = sc.textFile(args(7))
      .filter(_.length!=0).filter(!_.contains("RI"))
      .filter(line=>{
        val lineList = Utils.splite(line)
        if(lineList.size() != 2){
          false
        }else {
          var flag =true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          flag
        }
      }).map(line=>{
      val lineList = Utils.splite(line)
      if(lineList.get(1).equals("AAA")){
        (lineList.get(0),("AAA"))
      }else if(lineList.get(1).equals("AA+")||lineList.get(1).equals("A-1+")||lineList.get(1).equals("AA＋")){
        (lineList.get(0),"AA+")
      }else{
        (lineList.get(0),"AA及以下")
      }
    }).collectAsMap()
    val broadRiCreditRtng = sc.broadcast(ri_credit_rtng)

    println("file 8 ri_credit_rtng read succeed! read "+ ri_credit_rtng.size+" records")

    //用于找出银银间交易
    val dps_v_cr_dep_txn_dtl_data = sc.textFile(args(8))
      .filter(_.length!=0).filter(!_.contains("DL")).filter(line=>{
        val lineList = Utils.splite(line)
        if(lineList.size() != 2){
          false
        }else {
          var flag =true
          for(i<-0 until lineList.size() if flag){
            if(lineList.get(i)==null || lineList.get(i).isEmpty){
              flag =false
            }
          }
          if(!lineList.get(1).equals("1")){
            flag = false
          }
          flag
        }
      })
      .map(line=>{
        val lineList = Utils.splite(line)
        (lineList.get(0).trim,lineList.get(1).trim)
      }).collectAsMap()

    if(dps_v_cr_dep_txn_dtl_data.size>2000000){
      log.warn("dps_v_Cr_dep_txn_Dtl size is:" +dps_v_cr_dep_txn_dtl_data.size)
    }
   val broadDpsDepTxnDtlData = sc.broadcast(dps_v_cr_dep_txn_dtl_data)
    println("file 9 dps_v_cr_dep_txn_dtl read succeed! read "+ dps_v_cr_dep_txn_dtl_data.size+" records")
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
    val deal_cannot_find_trdngRiID = sc.longAccumulator("deal_cannot_find_trdngRiID")
    val deal_cannot_find_pdID  = sc.longAccumulator("deal_cannot_find_pdID")

    val bond_repo_deal_pd_filter_table= bond_repo_deal_f_rdd
      .map(x=>{
        val repo_pty_uniq = x._2._1
        val rvrse_repo_uniq = x._2._2
        val trdng_pd_id = x._2._3
        var pd_cd = "None"
        if(!broadPD_D.value.contains(trdng_pd_id)){
          deal_cannot_find_pdID.add(1l)
          log.error("pd_d table does not contain this pd_id:"+trdng_pd_id)
        }else{
           pd_cd = broadPD_D.value.get(trdng_pd_id).get //根据trdng_pd_id得到 pd_cd
        }
        val deal_number = x._2._4
        val trdng_ri_id = x._2._5
        var rtng_desc = "None"
        if(!broadRiCreditRtng.value.contains(trdng_ri_id)){
          deal_cannot_find_trdngRiID.add(1l)
          log.error("sor.ri_credit_rtng table does not contain this trdng_ri_id:"+trdng_ri_id)
        }else{
          rtng_desc = broadRiCreditRtng.value.get(x._2._5).get
        }
        val deal_dt = x._2._7
        val deal_repo_rate = x._2._8
        val trade_amnt = x._2._9
        (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
      }
    ).filter(record=>{
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
//(pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
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
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(root_member_uniq.equals(member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2))
        .mapValues(x=>Tuple2(x._1/x._2,x._2)).cache()

    root_repo_pty_day  //将正回购结果写Hbase!!!
      .map(result=>{
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(result._1._4)
      (result._1._1,result._1._2,result._1._3,deal_date, result._2._1.bigDecimal,result._2._2.bigDecimal)
    })
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "REPO_WEIGHTED_PRICE","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
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
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      })
      .reduceByKey((x,y)=>Tuple2(x._1+y._1, x._2+y._2)) //将结果写HBase!!
        .mapValues(x=>Tuple2(x._1/x._2,x._2)).cache() // 结果为<sum(加权价)/sum(amount),sum(amount)>

    root_rvrse_repo_pty_day //将逆回购结果写Hbase!!!
      .map(result=>{
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(result._1._4)
      (result._1._1,result._1._2,result._1._3,deal_date, result._2._1.bigDecimal,result._2._2.bigDecimal)
    })
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "RVRSE_WEIGHTED_PRICE","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /*
      正逆回购间价差表
    */
    root_repo_pty_day.join(root_rvrse_repo_pty_day) //!!!!将结果写Hbase.
      .mapValues(x=>{
      val repo = x._1
      val rvrse = x._2
      Tuple2 (rvrse._1 - repo._1, repo._2+ rvrse._2)//加权价(逆-正)相减 得到价差，amount相加得 到总amount
    })
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        (result._1._1,result._1._2,result._1._3,result._1._4, result._2._1.bigDecimal,result._2._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_PRICE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "PD_CD","DEAL_DT", "PRICE_DIFFERENCE","TOTAL_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /*
       机构类型(展示类型，非机构真正的类型)   正回购 统计 （对汇总结果按类型汇总!）
      */
    val ctgry_repo_pty_day = repo_pty_day.map(record=>{
      val member_uniq = record._1._1 //得到机构ID
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }      //key: 机构类型，汇总类型，产品，日期
      (Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
      .mapValues(x=>x._1/x._2).cache() //sum(加权价)/sum(amount) ,无需保持总量

    //结果 正回购价写表
      ctgry_repo_pty_day.map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        (result._1._1,result._1._2,deal_date,result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
      "REPO_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))

    /*
       机构类型(展示类型，非机构真正的类型) 逆回购 统计 （对汇总结果按类型汇总!）
      */
    val ctgry_rvrse_repo_pty_day = rvrse_repo_pty_day.map(record=>{
      val member_uniq = record._1._1 //得到机构uiq
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }
      //key: 机构类型，汇总类型，产品，日期
      (Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>Tuple2(x._1+y._1,x._2+y._2))
      .mapValues(x=>x._1/x._2).cache() //sum(加权价)/sum(amount)

    //结果 逆回购价写表
      ctgry_rvrse_repo_pty_day.map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        (result._1._1,result._1._2,deal_date,result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))

    //结果 逆-正 价差写表
    ctgry_repo_pty_day.join(ctgry_rvrse_repo_pty_day)
      .mapValues(x=>{x._2 -x._1}) //逆回购-正回购
        .map(x=>{
         val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
         val deal_date = oracleDateFormat.parse(x._1._3)
          (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
      })
       .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "PRICE_DIFFERENCE"),conf,Some(ZOOKEEPER_URL))

    //正回购 按类型 单条记录汇总(类型,产品,日期)
    val ctgryRepoRecords = bond_repo_deal_pd_filter_table.map(x=>{
      val member_uniq = x._3
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }
      Tuple2(Tuple3(member_ins_show_nm,x._1,x._2),x._7)}).cache()

    //正回购 按类型最大 写表
    ctgryRepoRecords.reduceByKey((x,y)=>{
      if(x>y){
        x
      }else{
        y
      }
    }).map(x=>{
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(x._1._3)
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    //正回购 按类型最小  写Hbase
    ctgryRepoRecords.reduceByKey((x,y)=>{
      if(x<y){
        x
      }else{
        y
      }
    }).map(x=>{
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(x._1._3)
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "REPO_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    //逆回购 单条交易汇总
    val ctgryRvrseRepoRecords = bond_repo_deal_pd_filter_table.map(x=>{
      val member_uniq = x._4
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }
      Tuple2(Tuple3(member_ins_show_nm,x._1,x._2),x._7)}).cache()

    //逆回购 类型最大 写Hbase
    ctgryRvrseRepoRecords.reduceByKey((x,y)=>{
      if(x>y){
        x
      }else{
        y
      }
    }).map(x=>{
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(x._1._3)
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))
    //逆回购 类型最小 写Hbase
    ctgryRvrseRepoRecords.reduceByKey((x,y)=>{
      if(x<y){
        x
      }else{
        y
      }
    }).map(x=>{
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(x._1._3)
      (x._1._1,x._1._2,deal_date,x._2.bigDecimal)
    })
      .saveToPhoenix("CTGRY_PRICE_DEAL_INFO",Seq("INS_SHOW_NAME","PD_CD","DEAL_DT",
        "RVRSE_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))
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
      .map(x=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(x._1._2)
        (x._1._1,deal_date ,x._2.bigDecimal)})
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
          "MARKET_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))

    // 求最大和最小
    val market_repo_rates = bond_repo_deal_pd_filter_table.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._7
      Tuple2 (Tuple2(pd_cd,deal_dt), repo_rate)
    }).cache
    //全市场 最大 和最小 repo_rate 求PD_CD, DEAl_DT
    market_repo_rates.reduceByKey((x,y)=>{if(x>y)x else y})
      .map(x=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(x._1._2)
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最大
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
        "MARKET_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    market_repo_rates.reduceByKey((x,y)=>{if(x<y)x else y})
      .map(x=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(x._1._2)
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最小
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "MARKET_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    //银银间 正逆 回购 加权价，最大 和最小 repo_rate
    //首先过滤数据
    val banks_records =   bond_repo_deal_pd_filter_table
      .filter(record=>{
        val deal_number = record._5//成交编号
        var dpst_instn_trdng_indctr = "None"
        if(!broadDpsDepTxnDtlData.value.contains(deal_number)){
          log.error("broadDpsDepTxnDtlData does not contain deal_number:"+deal_number)
        }else{
          dpst_instn_trdng_indctr =  broadDpsDepTxnDtlData.value.get(deal_number).get
        }
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
      .map(x=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(x._1._2)
        (x._1._1,deal_date ,x._2.bigDecimal)})
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
        "BANKS_WEIGHTED_PRICE"),conf,Some(ZOOKEEPER_URL))

    //统计银银间 最大和最小 先将repo_rate统计出来
    val banks_repo_rates = banks_records.map(record=>{
      val pd_cd = record._1
      val deal_dt = record._2
      val repo_rate = record._7
      Tuple2 (Tuple2(pd_cd,deal_dt), repo_rate)
    }).cache
    //银银间最大 和最小 repo_rate 求PD_CD, DEAl_DT
    banks_repo_rates.reduceByKey((x,y)=>{if(x>y)x else y})
      .map(x=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(x._1._2)
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最大
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "BANKS_MAX_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    banks_repo_rates.reduceByKey((x,y)=>{if(x<y)x else y})
      .map(x=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(x._1._2)
        (x._1._1,deal_date ,x._2.bigDecimal)}) //最小
      .saveToPhoenix("GLOBAL_PRICE_DEAL_INFO",Seq("PD_CD","DEAL_DT",
      "BANKS_MIN_DEAL_REPO_RATE"),conf,Some(ZOOKEEPER_URL))

    //得到一笔交易的bond_id,首先将 ev_cltrl_dtls的 ri_id 与bond_d的 bond_id 相join
    //1.对 ev_cltrl_dtls进行预处理得到 (ev_id,(bond_id,cnvrsn_prprtn))
    val ev_cltrl_dtls = sc.textFile(args(9))
      .filter(_.length!=0).filter(!_.contains("EV"))
      .filter(line=>{
        val lineList = Utils.splite(line)
        if(lineList.size() != 3){
          false
        }else {
          var flag = true
          for(i<-0 until lineList.size() if flag){
            if( (lineList.get(i)==null || lineList.get(i).isEmpty) && (i!=4||i!=5) ){flag =false}
          }
          flag
        }
      }).map(line=>{
      val lineList = Utils.splite(line)
      Tuple2(lineList.get(1).trim,Tuple2(lineList.get(0).trim,BigDecimal.apply(lineList.get(2).trim)))
    })

    val repo_ev_joined_dtls = bond_repo_deal_f_rdd.map(x=>{
        val ev_id =x._1
        val repo_pty_uniq = x._2._1
        val rvrse_repo_uniq = x._2._2
        val trdng_pd_id = x._2._3
        var pd_cd = "None"
        if(!broadPD_D.value.contains(trdng_pd_id)){
          deal_cannot_find_pdID.add(1l)
          log.error("pd_d table does not contain this pd_id:"+trdng_pd_id)
        }else{
          pd_cd = broadPD_D.value.get(trdng_pd_id).get //根据trdng_pd_id得到 pd_cd
        }
        val deal_number = x._2._4
        val trdng_ri_id = x._2._5
        var rtng_desc = "None"
        if(!broadRiCreditRtng.value.contains(trdng_ri_id)){
          deal_cannot_find_trdngRiID.add(1l)
          log.error("sor.ri_credit_rtng table does not contain this trdng_ri_id:"+trdng_ri_id)
        }else{
          rtng_desc = broadRiCreditRtng.value.get(x._2._5).get
        }
        val deal_dt = x._2._7
        val deal_repo_rate = x._2._8
        val trade_amount = x._2._9
        (ev_id,(pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amount)))
      }).join(ev_cltrl_dtls)
      .map(x=>{
        val pd_cd = x._2._1._1
        val deal_dt = x._2._1._2
        val repo_pty_uniq = x._2._1._3
        val rvrse_repo_uniq = x._2._1._4
         val deal_number = x._2._1._5
         val  rtng_desc= x._2._1._6
           val deal_repo_rate = x._2._1._7
             val trade_amount =x._2._1._8
        val bond_id = x._2._2._1
        val cnvrsn_prprtn = x._2._2._2
        (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amount,bond_id,cnvrsn_prprtn)
      })

    /*
      正逆回购 在各质押物类型上的分布 root机构 参与者类型 质押物类型，每天，amount累加
      需要统计所有产品上的信息，所以用 bond_repo_deal_table
     */
  //  1      2     3                4              5           6          7            8           9         10
// (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amount,bond_id,cnvrsn_prprtn)
    //正回购机构，质押物类型，每天，amount
    val repo_zhiyawuleixing_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._3 //正回购方
      val deal_date = record._2
      val trade_amount = record._8
      var bond_id = record._9
      var bond_ctgry_nm = "None"
      if(!broadBondD.value.contains(bond_id)){
        log.error("broadBondD does not contain bond_id:"+bond_id)
      }else{
        bond_ctgry_nm = broadBondD.value.get(bond_id).get._1 //得到债券分类
      }
    Tuple2(Tuple3(member_uniq, bond_ctgry_nm, deal_date), trade_amount)  //机构ID，质押物类型，日期，amount
  }).cache()

    //正 root机构，参与者类型，质押物类型，每天，amount
    repo_zhiyawuleixing_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      var root_member_uniq = "None"
      if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
        log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
      }else{
        root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
      }
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
      }else{
        arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        var member_full_name = "None"
        if(!broadMEMBER_D.value.contains(member_uniq)){
          log.error("broadMEMBER_D doesnot contain:"+member_uniq)
        }else{
          member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
        }
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_CTGRY_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    repo_zhiyawuleixing_rdd.map(record=>{
      val repo_member_uniq = record._1._1
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(repo_member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+repo_member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(repo_member_uniq).get
      }
      Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        (result._1._1,result._1._2,deal_date, result._2.bigDecimal)
       // (result._1._1,result._1._2,result._1._3, result._2)
      })
      .saveToPhoenix("CTGRY_BOND_CTGRY_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","DEAL_DT",
        "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))
    /*
    统计逆回购的相关信息
     */
    val rvrse_repo_zhiyawuleixing_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._4 //逆回购方
      val deal_date = record._2
      val trade_amount = record._8
      var bond_id = record._9
      var bond_ctgry_nm = "None"
      if(!broadBondD.value.contains(bond_id)){
        log.error("broadBondD does not contain bond_id:"+bond_id)
      }else{
        bond_ctgry_nm = broadBondD.value.get(bond_id).get._1 //得到债券分类
      }
      Tuple2(Tuple3(member_uniq, bond_ctgry_nm, deal_date), trade_amount)  //机构ID，质押物类型，日期，amount
    }).cache()

    //统计 根机构信息,得到每个机构的在每个分布上的amout之和     结果写HBase！！！！！
    rvrse_repo_zhiyawuleixing_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_CTGRY_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","DEAL_DT", "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //统计 机构类型信息,得到每个机构的在每个分布上的amout之和    结果写HBase!!!!!!
    rvrse_repo_zhiyawuleixing_rdd
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        if(!broadMemberUniq2Name.value.contains(member_uniq)){
          log.error("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        (result._1._1,result._1._2,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_CTGRY_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM","DEAL_DT",
        "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /* 正回购
      机构在 评级上的分布 根机构机构 信用评级，每天，amount累加
    //  1      2     3                4              5           6          7            8           9         10
// (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amount,bond_id,cnvrsn_prprtn)
    //正回购机构，质押物类型，每天，amount
     */
    val repo_bond_credit_deal_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._3
      val deal_date = record._2
      val rtng_desc = record._6
      val trade_amount = record._8
      Tuple2(Tuple3(member_uniq,rtng_desc,deal_date),trade_amount)
      //机构ID，信用类型，日期，amount
    }).cache()

    // 根机构 评级统计 结果 写Hbase!!!!!
    repo_bond_credit_deal_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_RTNG_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "RTNG_DESC","DEAL_DT", "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    // 机构类型 评级统计 结果 写Hbase!!!!!
    repo_bond_credit_deal_rdd
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        if(!broadMemberUniq2Name.value.contains(member_uniq)){
          log.error("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        (result._1._1,result._1._2,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_RTNG_DEAL_INFO",Seq("INS_SHOW_NAME", "RTNG_DESC","DEAL_DT",
        "REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /*
    逆回购 在质押物类型上的分布
     */
    val rvrse_repo_bond_credit_deal_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._4
      val deal_date = record._2
      val rtng_desc = record._6
      val trade_amount = record._8
      Tuple2(Tuple3(member_uniq,rtng_desc,deal_date),trade_amount)
      //机构ID，信用类型，日期，amount
    }).cache()

    // 根机构 评级统计 结果 写Hbase!!!!!
    rvrse_repo_bond_credit_deal_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        (result._1._1,result._1._2,result._1._3,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_RTNG_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "RTNG_DESC","DEAL_DT", "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    // 机构类型 评级统计 结果 写Hbase!!!!!
    rvrse_repo_bond_credit_deal_rdd
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        if(!broadMemberUniq2Name.value.contains(member_uniq)){
          log.error("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>{x+y})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        (result._1._1,result._1._2,deal_date, result._2.bigDecimal)
      })
      .saveToPhoenix("CTGRY_BOND_RTNG_DEAL_INFO",Seq("INS_SHOW_NAME", "RTNG_DESC","DEAL_DT",
        "RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /*
     质押物折扣率走势图检索:债券类型检索
  //  1      2      3               4              5           6          7            8            9         10
// (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amount,bond_id,cnvrsn_prprtn)
 */
    val repo_bond_discount_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._3 //正回购方
      val deal_date = record._2
      val trade_amount = record._8
      val rtng_desc = record._6
      val bond_id = record._9
      val cnvrsn_prprtn = record._10
      var bond_ctgry_nm = "None"
      if(!broadBondD.value.contains(bond_id)){
        log.error("broadBondD does not contain bond_id:"+bond_id)
      }else{
        bond_ctgry_nm = broadBondD.value.get(bond_id).get._1 //得到债券分类
      }
      Tuple2(Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date),Tuple2(cnvrsn_prprtn* trade_amount,trade_amount))
      //key:机构ID，质押物类型，评级，日期； value：折扣率，amount
    }).cache()

    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"总额",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"自营",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._5)
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,result._1._3,result._1._4,deal_date,RWCP,result._2._2.bigDecimal)})
      .saveToPhoenix("MEMBER_BOND_TYPE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE", "BOND_CTGRY_NM",
        "RTNG_DESC","DEAL_DT", "REPO_WEIGHTED_CNVRSN_PRPRTN","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }
      Tuple2(Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,result._1._3,deal_date,RWCP)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_TYPE_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM",
        "RTNG_DESC","DEAL_DT", "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    /*
    对逆回购进行分析
*/
    val rvrse_repo_bond_discount_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._4 //逆回购方
      val deal_date = record._2
      val trade_amount = record._8
      val rtng_desc = record._6
      val bond_id = record._9
      val cnvrsn_prprtn = record._10
      var bond_ctgry_nm = "None"
      if(!broadBondD.value.contains(bond_id)){
        log.error("broadBondD does not contain bond_id:"+bond_id)
      }else{
        bond_ctgry_nm = broadBondD.value.get(bond_id).get._1 //得到债券分类
      }
      Tuple2(Tuple4(member_uniq,bond_ctgry_nm,rtng_desc,deal_date),Tuple2(cnvrsn_prprtn* trade_amount,trade_amount))
      //key:机构ID，质押物类型，评级，日期； value：折扣率，amount
    }).cache()

    ///按根机构 进行统计 结果写Hbase
    rvrse_repo_bond_discount_rdd
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple5[String,String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
        arrays += Tuple2(Tuple5 (root_member_uniq,"总额",record._1._2,record._1._3,record._1._4), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple5 (root_member_uniq,"自营",record._1._2,record._1._3,record._1._4), record._2)
        }else{
          arrays += Tuple2(Tuple5 (root_member_uniq,"产品",record._1._2,record._1._3,record._1._4), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name = broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple5 (root_member_uniq,"货币市场基金",record._1._2,record._1._3,record._1._4), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._5)
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,result._1._3,result._1._4,deal_date, RWCP,result._2._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_TYPE_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "BOND_CTGRY_NM","RTNG_DESC","DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //按机构类型 进行统计 结果写Hbase
    rvrse_repo_bond_discount_rdd.map(record=>{
      val member_uniq = record._1._1
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }
      Tuple2(Tuple4(member_ins_show_nm,record._1._2,record._1._3,record._1._4),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._4)
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,result._1._3,deal_date,RWCP)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_TYPE_DEAL_INFO",Seq("INS_SHOW_NAME", "BOND_CTGRY_NM",
      "RTNG_DESC","DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))
    /*
     质押物折扣率走势图检索:发行机构检索     正回购
   //  1      2      3               4              5           6          7            8            9         10
// (pd_cd,deal_dt,repo_pty_uniq,rvrse_repo_uniq,deal_number,rtng_desc,deal_repo_rate,trade_amount,bond_id,cnvrsn_prprtn)

 */
    val repo_bond_discount_issr_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._3 //正回购方
      val deal_date = record._2
      val trade_amount = record._8
      val rtng_desc = record._6
      val bond_id = record._9
      val cnvrsn_prprtn = record._10
      var issur_nm ="None"
      if(broadBondD.value.contains(bond_id)){
        issur_nm = broadBondD.value.get(bond_id).get._3
      }else{
        log.error("broadBondD does not contain bond_id:"+bond_id)
      }
      //key:          机构ID，  发行人名称，日期；             value：加权折扣率，amount
      Tuple2(Tuple3(member_uniq,issur_nm,deal_date),Tuple2(cnvrsn_prprtn*trade_amount,trade_amount))
    }).cache()

    ///按根机构 进行统计 结果写Hbase
    repo_bond_discount_issr_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      var root_member_uniq = "None"
      if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
        log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
      }else{
        root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
      }
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
      }else{
        arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        var member_full_name = "None"
        if(!broadMEMBER_D.value.contains(member_uniq)){
          log.error("broadMEMBER_D doesnot contain:"+member_uniq)
        }else{
          member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
        }
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(record._1._4)
        val RWCP = (record._2._1/record._2._2).bigDecimal
        (record._1._1,record._1._2,record._1._3,deal_date,RWCP,record._2._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_ISSR_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "ISSR_NM","DEAL_DT","REPO_WEIGHTED_CNVRSN_PRPRTN","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //按机构类型 进行统计 结果写Hbase
    repo_bond_discount_issr_rdd.map(record=>{
      val mem_uniq = record._1._1
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(mem_uniq)){
        log.error("broadMemberUniq2Name does not contain "+mem_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(mem_uniq).get
      }
      Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        val RWCP = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,deal_date,RWCP)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_ISSR_DEAL_INFO",Seq("INS_SHOW_NAME", "ISSR_NM","DEAL_DT",
      "REPO_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    val rvrse_repo_bond_discount_issr_rdd = repo_ev_joined_dtls.map(record=>{
      val member_uniq = record._4 //正回购方
      val deal_date = record._2
      val trade_amount = record._8
      val rtng_desc = record._6
      val bond_id = record._9
      val cnvrsn_prprtn = record._10
      var issur_nm ="None"
      if(broadBondD.value.contains(bond_id)){
        issur_nm = broadBondD.value.get(bond_id).get._3
      }else{
        log.error("broadBondD does not contain bond_id:"+bond_id)
      }
      //key:          机构ID，  发行人名称，日期；             value：加权折扣率，amount
      Tuple2(Tuple3(member_uniq,issur_nm,deal_date),Tuple2(cnvrsn_prprtn*trade_amount,trade_amount))
    }).cache()

    ///按根机构 进行统计 结果写Hbase
    rvrse_repo_bond_discount_issr_rdd.flatMap(record=>{
      val member_uniq = record._1._1
      var root_member_uniq = "None"
      if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
        log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
      }else{
        root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
      }
      val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Tuple2[BigDecimal,BigDecimal])]()
      arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
      if(member_uniq.equals(root_member_uniq)){
        arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
      }else{
        arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
        var member_full_name = "None"
        if(!broadMEMBER_D.value.contains(member_uniq)){
          log.error("broadMEMBER_D doesnot contain:"+member_uniq)
        }else{
          member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
        }
        if(member_full_name.contains("货币市场基金")){
          arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
        }
      }
      arrays.iterator
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(record=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(record._1._4)
        val RWCP = (record._2._1/record._2._2).bigDecimal
        (record._1._1,record._1._2,record._1._3,deal_date,RWCP,record._2._2.bigDecimal)
      })
      .saveToPhoenix("MEMBER_BOND_ISSR_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE", "ISSR_NM",
        "DEAL_DT","RVRSE_WEIGHTED_CNVRSN_PRPRTN","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //按机构类型 进行统计 结果写Hbase
   rvrse_repo_bond_discount_issr_rdd.map(record=>{
       val mem_uniq = record._1._1
       var member_ins_show_nm = "None"
       if(!broadMemberUniq2Name.value.contains(mem_uniq)){
         log.error("broadMemberUniq2Name does not contain "+mem_uniq)
       }else{
         member_ins_show_nm = broadMemberUniq2Name.value.get(mem_uniq).get
       }
     Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
    }).reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)})
      .map(result=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(result._1._3)
        val value = (result._2._1/result._2._2).bigDecimal
        (result._1._1,result._1._2,deal_date,value)
      })//无需写amount
      .saveToPhoenix("CTGRY_BOND_ISSR_DEAL_INFO",Seq("INS_SHOW_NAME", "ISSR_NM",
     "DEAL_DT", "RVRSE_WEIGHTED_CNVRSN_PRPRTN"),conf,Some(ZOOKEEPER_URL))

    /*
    机构交易 分布图例
     原始输入数据 (pd_cd,deal_dt,repo_pty_id,rvrse_repo_pty,deal_number,rtng_desc,BigDecimal.apply(deal_repo_rate)
          ,BigDecimal.apply(trade_amnt), x._1)
     结果 输出数据。（根）机构ID，参与者类型,对比机构类型,日期,交易金额
     */
    //考虑正回购信息
    val repo_pty_distribution_day= repo_ev_joined_dtls.map(record=>{
      val deal_date = record._2
      val member_uniq = record._3 //正回购方
      val rvrMember_uniq = record._4
      val trade_amount = record._8
      var rvrInsShowName = "None"
      if(!broadMemberUniq2Name.value.contains(rvrMember_uniq)){
        log.error("broadMemberUniq2Name does not contain: member uniq:"+rvrMember_uniq)
      }else{
        rvrInsShowName = broadMemberUniq2Name.value.get(rvrMember_uniq).get
      }
      //key:          机构ID，  对手机构类型，          日期；    value：amount
      Tuple2(Tuple3(member_uniq,rvrInsShowName,deal_date),trade_amount)
    }).cache()

    //正回购 按机构进行统计，结果写Hbase!（注意：由于结果不分 正逆，因此 需要跟 逆的结果进行合并）
    repo_pty_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
    //    Tuple2(Tuple3(root_member_uniq,record._1._2,record._1._3),record._2)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x+y)) //key: ID,参与者类型,对比机构类型,日期 value:amount
        .map(record=>{
         val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
         val deal_date = oracleDateFormat.parse(record._1._4)
         val value = record._2.bigDecimal
          (record._1._1,record._1._2,record._1._3,deal_date,value)})
        .saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //正回购 按机构类型统计，结果写Hbase!
    repo_pty_distribution_day
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        if(!broadMemberUniq2Name.value.contains(member_uniq)){
          log.error("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>(x+y)) //
      .map(record=>{
         val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
          val deal_date = oracleDateFormat.parse(record._1._3)
          val value = record._2.bigDecimal
         (record._1._1,record._1._2,deal_date,value)})
      .saveToPhoenix("CTGRY_CTGRY_AMOUNT_DEAL_INFO",Seq("INS_SHOW_NAME",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //考虑逆回购 信息

    val rvrse_repo_pty_distribution_day = repo_ev_joined_dtls.map(record=>{
      val deal_date = record._2
      val member_uniq = record._3 //正回购方
      val rvrMember_uniq = record._4
      val trade_amount = record._8
      var memInsShowName = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain: member uniq:"+member_uniq)
      }else{
        memInsShowName = broadMemberUniq2Name.value.get(member_uniq).get
      }
      //key:          机构ID，  对手机构，          日期；    value：amount
      Tuple2(Tuple3(rvrMember_uniq,memInsShowName,deal_date),trade_amount)
    }).cache()

    //逆回购 按机构统计
    rvrse_repo_pty_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
       // Tuple2(Tuple3(root_member_uniq,record._1._2,record._1._3),record._2)
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],BigDecimal)]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4 (root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.iterator
      }).reduceByKey((x,y)=>(x+y)) //key: ID,参与者类型,对比机构类型,日期 value:amount
      .map(record=>{
          val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
          val deal_date = oracleDateFormat.parse(record._1._4)
          val value = record._2.bigDecimal
          (record._1._1,record._1._2,record._1._3,deal_date,value)
      })
      .saveToPhoenix("MEMBER_CTGRY_AMOUNT_DEAL_INFO",Seq("UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    //逆回购 按机构类型统计，结果写Hbase!
    rvrse_repo_pty_distribution_day
      .map(record=>{
        val member_uniq = record._1._1
        var member_ins_show_nm = "None"
        if(!broadMemberUniq2Name.value.contains(member_uniq)){
          log.error("broadMemberUniq2Name does not contain :"+member_uniq)
        }else{
          member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
        }
        Tuple2(Tuple3(member_ins_show_nm,record._1._2,record._1._3),record._2)
      }).reduceByKey((x,y)=>(x+y))
      .map(record=>{
        val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
        val deal_date = oracleDateFormat.parse(record._1._3)
        val value = record._2.bigDecimal
        (record._1._1,record._1._2,deal_date,value)
      })
      .saveToPhoenix("CTGRY_CTGRY_AMOUNT_DEAL_INFO",Seq("INS_SHOW_NAME",
        "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_SUM_AMOUNT"),conf,Some(ZOOKEEPER_URL))

    /*
        分布图 考虑在 各机构类型（机构数）上的分布
     */
    //考虑 机构 正回购信息

    val repo_pty_jigou_distribution_day =  repo_ev_joined_dtls.map(record=>{
      val deal_date = record._2
      val member_uniq = record._3 //正回购方
      val rvrMember_uniq = record._4 //逆回购方
      var rvrInsShowName = "None"
      if(!broadMemberUniq2Name.value.contains(rvrMember_uniq)){
        log.error("broadMemberUniq2Name does not contain: member uniq:"+rvrMember_uniq)
      }else{
        rvrInsShowName = broadMemberUniq2Name.value.get(rvrMember_uniq).get
      }
      val rvrSet = Set(rvrMember_uniq)
      Tuple2(Tuple3(member_uniq,rvrInsShowName,deal_date),rvrSet)
    }).cache()

    repo_pty_jigou_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Set[String])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4(root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>{x++y}) //得到 每个机构的结果集
      .map(record=>{
        val member_uniq =  record._1._1
        var member_ins_show_nm = "None"
        if(!broadMemberUniq2Name.value.contains(member_uniq)){
          log.error("broadMemberUniq2Name does not contain "+member_uniq)
        }else{
          member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
        }
        val memberSetString = record._2.mkString(",")
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(record._1._4)
        (member_ins_show_nm,member_uniq,record._1._2,record._1._3,deal_date, memberSetString)
    }).saveToPhoenix("MEMBER_CTGRY_NUMBER_DEAL_INFO",Seq("INS_SHOW_NAME","UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","REPO_COU_PARTY_UNIQ_SET"),conf,Some(ZOOKEEPER_URL))

    //考虑 机构 逆回购信息
    val rvrse_repo_pty_jigou_distribution_day = repo_ev_joined_dtls.map(record=>{
      val deal_date = record._2
      val member_uniq = record._3 //正回购方
      val rvrMember_uniq = record._4 //逆回购方
      var memInsShowName = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){ //此时对手机构 为正回购
        log.error("broadMemberUniq2Name does not contain: member uniq:"+member_uniq)
      }else{
        memInsShowName = broadMemberUniq2Name.value.get(member_uniq).get
      }
      val memSet = Set(member_uniq)
      Tuple2(Tuple3(rvrMember_uniq,memInsShowName,deal_date),memSet)
    }).cache()

    //逆回购 对手机构集合统计
    rvrse_repo_pty_jigou_distribution_day
      .flatMap(record=>{
        val member_uniq = record._1._1
        var root_member_uniq = "None"
        if(!broadMemberUniq2RootUniq.value.contains(member_uniq)){
          log.error("broadMemberUniq2RootUniq doesnot contain member_uniq "+member_uniq)
        }else{
          root_member_uniq = broadMemberUniq2RootUniq.value.get(member_uniq).get //根据映射找出其父机构ID
        }
        val arrays = new ArrayBuffer[(Tuple4[String,String,String,String],Set[String])]()
        arrays += Tuple2(Tuple4 (root_member_uniq,"总额",record._1._2,record._1._3), record._2)
        if(member_uniq.equals(root_member_uniq)){
          arrays += Tuple2(Tuple4(root_member_uniq,"自营",record._1._2,record._1._3), record._2)
        }else{
          arrays += Tuple2(Tuple4 (root_member_uniq,"产品",record._1._2,record._1._3), record._2)
          var member_full_name = "None"
          if(!broadMEMBER_D.value.contains(member_uniq)){
            log.error("broadMEMBER_D doesnot contain:"+member_uniq)
          }else{
            member_full_name= broadMEMBER_D.value.get(member_uniq).get._3//得到机构全称
          }
          if(member_full_name.contains("货币市场基金")){
            arrays += Tuple2(Tuple4 (root_member_uniq,"货币市场基金",record._1._2,record._1._3), record._2)
          }
        }
        arrays.toIterator
      }).reduceByKey((x,y)=>{x++y}) //得到 每个机构的结果集
      .map(record=>{
      val member_uniq =  record._1._1
      var member_ins_show_nm = "None"
      if(!broadMemberUniq2Name.value.contains(member_uniq)){
        log.error("broadMemberUniq2Name does not contain "+member_uniq)
      }else{
        member_ins_show_nm = broadMemberUniq2Name.value.get(member_uniq).get
      }
      val memberSetString = record._2.mkString(",")
      val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val deal_date = oracleDateFormat.parse(record._1._4)
      (member_ins_show_nm,member_uniq,record._1._2,record._1._3,deal_date, memberSetString)
    }).saveToPhoenix("MEMBER_CTGRY_NUMBER_DEAL_INFO",Seq("INS_SHOW_NAME","UNQ_ID_IN_SRC_SYS","JOIN_TYPE",
      "COU_PARTY_INS_SHOW_NAME","DEAL_DT","RVRSE_COU_PARTY_UNIQ_SET"),conf,Some(ZOOKEEPER_URL))
  }
}
