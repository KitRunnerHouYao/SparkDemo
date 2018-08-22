import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.phoenix.spark._
import org.apache.spark.{SparkConf, SparkContext}
object gz_project {

    val sparkconf = new SparkConf().setAppName("gz_project").setMaster("local[2]") //！！！集群上改！！！
    val sc = new SparkContext(sparkconf)
    val ZOOKEEPER_URL="127.0.0.1:2181"
    @transient lazy val  log = Logger.getLogger(this.getClass)

    /**
      * args(0)=  SOR.EV_CLTRL_DTLS
      * args(1)=SOR.BOND_REPO_DEAL
      * args(2) = TRDX_DEAL_INFRMN_RMV_D
      * args(3) = SOR.ORG
      * args(4) = BOND_D
      * args(5) = DPS_V_CBT_ALL_TXN_DTL
      * args(6) = AFTER_HOURS_BOND_TYPE
      * args(7) = MEMBER_D
      * args(8) = BASE_L_BONDSFINALVALUATION
      * args(9) = SOR.BASE_BENM_T_BONDINFO
      * args(10) = DPA.TRDNG_MTHD_D
      * args(11) = DPA.BOND_DEAL
      */

    def main(args: Array[String]): Unit = {
      val conf=new Configuration()
      /**
        * ****************************************************************
        * 临时表3
        * 只涉及两张表band_repo_deal   ev_cltrl_dtls
        */


      val path1 = "/Users/zcg/svn/ev_cltrl_dtls.csv"
      val path2 = "/Users/zcg/svn/bond_repo_deal.csv"
      val path3 = "/Users/zcg/svn/TRDX_DEAL_INFRMN_RMV_D.csv"
      val path4 = "/Users/zcg/svn/ORG.csv"
      val path5 = "/Users/zcg/svn/BOND_D.csv"
      val path6 = "/Users/zcg/svn/DPS_V_CBT_ALL_TXN_DTL.csv"
      val path7 = "/Users/zcg/svn/AFTER_HOURS_BOND_TYPE.csv"
      val path8 = "/Users/zcg/svn/MEMBER_D.csv"
      val path9 = "/Users/zcg/svn/BASE_BENM_T_BONDINFO.csv"
      val path10 = "/Users/zcg/svn/BASE_L_BONDSFINALVALUATION.csv"
      val path11 = "/Users/zcg/svn/TRDNG_MTHD_D.csv"
      val path12 = "/Users/zcg/svn/BOND_DEAL.csv"


      /**
        * band_repo_deal
        * EV_ID,TRADE_AMNT,DEAL_DT
        * ev_id	repo_pty_id	RVRSE_REPO_PTY_ID	trdng_pd_id	deal_nmbr	trdng_ri_id	ST	deal_dt	DEAL_REPO_RATE	Trade_amnt  mkt_id
        * EV_ID:关联条件   TRADE_AMNT：质押式回购成交金额  DEAL_DT：成交日期
        * 计算公式：zt_rate=NVL(SUM(CNVRSN_PRPRTN*TRADE_AMNT) / SUM(TRADE_AMNT),0)
        *
        */
     val band_repo_deal_data = sc.textFile(path2)
        .filter(_.length!=0).filter(!_.contains("REPO"))
        .filter(line => {
          val lineArray =Utils.splite(line)
          if (lineArray.size() != 9) {   //需改成11
            false
          } else {
            true
          }
        })
        .map(line => {
          val tmp = Utils.splite(line)
          val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
          val deal_dt = oracleDateFormat.parse(tmp.get(5))    //7
          (tmp.get(0), (deal_dt,tmp.get(7)))  //9
        })
        // .collect.foreach(println)
        .collectAsMap()
     val broadBAND_REPO_DEAL=sc.broadcast(band_repo_deal_data)    //临时表三
      println("file 2 band_repo_deal read succeed! read "+band_repo_deal_data.size+" records")
      /**
        * 数据清洗  ev_cltrl_dtls
        * ri_id	ev_id	cnvrsn_prprtn total_face_value id..
        * 以EV_ID为key右关联表band_repo_deal.EV_ID
        * RI_ID：成交编号   CNVRSN_PRPRTN：价值比例
        */
      val ev_cltrl_dtls_data = sc.textFile(path1)
        .filter(_.length!=0).filter(!_.contains("EV"))    //csv文件第一行为表名
        .filter(line => {
        val lineArray = Utils.splite(line)
        if (lineArray.size() != 6) {   //5
          false
        } else {
          true
        }
      })
        .filter(line => {
          val lineArray = Utils.splite(line)
          lineArray.get(0)!="0"
        })
        .filter(line=>{
          val tmp =Utils.splite(line)
          broadBAND_REPO_DEAL.value.get(tmp.get(1))!=None
        })
        .map(line => {
          val tmp =Utils.splite(line)
          val data =broadBAND_REPO_DEAL.value.get(tmp.get(1))
        //  if(data!=None){}
          val deal_dt=data.get._1
          val Trade_amnt=data.get._2
          val ri_id=tmp.get(0)
          val cnvrsn_prprtn=BigDecimal(tmp.get(2))
          val total_face_value=BigDecimal(tmp.get(3))
          val datas=Set(ri_id,cnvrsn_prprtn,total_face_value)
          (tmp.get(0),tmp.get(1),deal_dt, total_face_value, cnvrsn_prprtn,Trade_amnt)
        })
        .cache()
       // .collect.foreach(println)

        val data_ev_tmp1=ev_cltrl_dtls_data.map(tmp=>{
          (tmp._1,tmp._4)
        })
        .reduceByKey((x,y)=>(x+y))
       .collectAsMap()
         // .collect.foreach(println)
      val broadEV_CLTRL_DTLS_tmp1 = sc.broadcast(data_ev_tmp1)

      val data_ev_tmp2=ev_cltrl_dtls_data.map(tmp=>{
        val data=broadEV_CLTRL_DTLS_tmp1.value.get(tmp._1).get
        val Trade_amnt=BigDecimal.apply(tmp._6)     //分母
        val rank_rird=tmp._4/data      //每个ri_id的total_face_value占比
        val ttl_fz=tmp._5*rank_rird*Trade_amnt   //加权分子
        var zyre=BigDecimal(0)
        if(Trade_amnt!=0 && Trade_amnt!=None && Trade_amnt!=null){
          zyre=ttl_fz/Trade_amnt
        }
        ((tmp._1,tmp._3),zyre)
      })
      // .collect.foreach(println)
          .collectAsMap()
      val broadEV_CLTRL_DTLS = sc.broadcast(data_ev_tmp2)
      println("file 1 ev_cltrl_dtls_data read succeed! read "+data_ev_tmp2.size+" records")


      /**
        * ***************************************************************************************
        *   * 临时表1
        */
      /**
        *TRDX_DEAL_INFRMN_RMV_D
        * dir_oprtn_type_indc	dir_rmv_status_indc	mkt_id	deal_id
        * 过滤条件：DIR_OPRTN_TYPE_INDC = 2 AND DIR_RMV_STATUS_INDC = 1 AND DEAL_ID = BOND_DEAL.BOND_DEAL_ID(保留)
        */
     val trdx_deal_infrmn_data=sc.textFile(path3)
        .filter(_.length!=0).filter(!_.contains("TRDX"))
        .filter(line=>{
          val lineArray =Utils.splite(line)
          if(lineArray.size()!=4){
            false
          }else{
            true
          }
        })
        .filter(line=>{
          val lineArray =Utils.splite(line)
          if(lineArray.get(0).equals(2)&& lineArray.get(1).equals("1")){
            false
          }else{
            true
          }
        })
        .map(line=>{
          val tmp=Utils.splite(line)
          (tmp.get(3),1)                       //过滤条件   存在及过滤
        })
        .reduceByKey((x,y)=>x+y)
        .collectAsMap()
      // .collect.foreach(println)
      val broadTRDX_DEAL_INFRMN = sc.broadcast(trdx_deal_infrmn_data)
      println("file 3 TRDX_DEAL_INFRMN_RMV_D read succeed! read "+trdx_deal_infrmn_data.size+" records")

      /**
        * SOR.ORG   将机构名称整合到bond_d中
        *CHN_FULL_NM	UNQ_ID_IN_SRC_SYS
        * 关联条件：ORG.UNQ_ID_IN_SRC_SYS=BOND_D.CSTDTN_BANK_ID
        */
       val org_data = sc.textFile(path4)
          .filter(_.length != 0).filter(!_.contains("ORG"))
          .filter(line => {
            val lineArray =Utils.splite(line)
            if (lineArray.size()!= 2) {
              false
            } else {
              true
            }
          })
          .map(line => {
            val tmp = Utils.splite(line)
            (tmp.get(1), tmp.get(0))
          })
          .collectAsMap()
        val broadORG = sc.broadcast(org_data)
        println("file 4 ORG read succeed! read "+org_data.size+" records")

        /**
          * bond_d   基础表   字段固定
          * 关联条件：ORG.UNQ_ID_IN_SRC_SYS=BOND_D.CSTDTN_BANK_ID
          * 字段 ："BOND_ID","BOND_NM","BOND_CD","BOND_CTGRY_ID","BOND_CTGRY_NM","FRST_VALUE_DT","MRTY_DT","SCND_BLNGL_DESC","CSTDTN_BANK_ID","TTL_FACE_VALUE"
          *       bond_id	bond_ctgry_nm	issr_id	issr_nm	bond_nm BOND_CD	BOND_CTGRY_ID	FRST_VALUE_DT	MRTY_DT	SCND_BLNGL_DESC	CSTDTN_BANK_ID	TTL_FACE_VALUE LSTNG_DT  bond_prnt_tp_nm
          *       0，1，2，3，4，5，6，7，8，9
          *       0，4，5，6，1，7，8，9，10，11
          */
        val mem_cannot_find__org_id = sc.longAccumulator("mem_cannot_find_ORG_ID")
        val bond_d_data = sc.textFile(path5)
          .filter(_.length != 0).filter(!_.contains("BOND"))
          .filter(line => {
            val lineArray =Utils.splite(line)
            if (lineArray.size() != 14) {
              false
            }else{
              true
            }
          })
          .map(line => {
            val tmp = Utils.splite(line)
            val data=broadORG.value
            var chn_full_nm="None"
            if(!data.contains(tmp.get(10))) {
              log.error("org does not contain org_id " + tmp.get(10))
              mem_cannot_find__org_id.add(1l)
            }else{
              chn_full_nm = data.get(tmp.get(10)).get
            }
  
            val  FRST_VALUE_DT=tmp.get(7)
            val  MRTY_DT= tmp.get(8)
            val  LSTNG_DT=tmp.get(12)
            var BOND_CTGRY_NM2="其他"
            if(tmp.get(1).equals("国债") || tmp.get(1).equals("政策性金融债") || tmp.get(1).equals("同业存单") ||  tmp.get(1).equals("地方政府债") || tmp.get(1).equals("企业债") || tmp.get(1).equals("中期票据")) {
              BOND_CTGRY_NM2 = tmp.get(1)
            }
            if(tmp.get(1).equals("短期融资券") || tmp.get(1).equals("超短期融资券")){
              BOND_CTGRY_NM2="短期融资券(包含超短融)"
            }
            (tmp.get(0),(tmp.get(4), tmp.get(5), tmp.get(6), tmp.get(1),FRST_VALUE_DT,MRTY_DT , tmp.get(9), chn_full_nm,tmp.get(11),LSTNG_DT,BOND_CTGRY_NM2))  //机构id替换为机构名称
          })
          .collectAsMap()
         // .collect.foreach(println)
        val broadBondD = sc.broadcast(bond_d_data)
        println("file 5 BOND_D read succeed! read "+bond_d_data.size+" records")

        /**
          * DPS_V_CBT_ALL_TXN_DTL
          * dbd_deal_deal_idnty	deal_strategy
          * 字段处理： CASE WHEN DEAL_STRATEGY IN ('0','4',null) THEN '正常' ELSE '策略性交易' END
          * 关联条件：dbd_deal_deal_idnty=bond_deal.deal_nmbr  (每条交易)
          */
          //deal_strategy 存在空值 不能过滤掉
        val dps_v_cbt_all_txn_ftl_data = sc.textFile(path6)
          .filter(_.length != 0).filter(!_.contains("DPS"))
          //
          .filter(line => {
            val lineArray =Utils.splite(line)
            if (lineArray.get(0)== null  || lineArray.get(0).isEmpty) {
              false
            } else {
              true
            }
          })
          .map(line => {
            val tmp =Utils.splite(line)
            var strategy="策略性交易"
            if(tmp.get(1).equals("0") || tmp.get(1).equals("4") || tmp.get(1)==null || tmp.get(1).equals("None")){
              strategy="正常"
            }
            (tmp.get(0), strategy)
          })
          .collectAsMap()
          //  .collect.foreach(println)
        val broadDPS_V_ALL = sc.broadcast(dps_v_cbt_all_txn_ftl_data)
        println("file 6 DPS_V_CBT_ALL_TXN_DTL read succeed! read "+dps_v_cbt_all_txn_ftl_data.size+" records")

        /**
          * AFTER_HOURS_BOND_TYPE  H
          * ins_show_name	member_ctgry_id
          * 关联表MEMBER_D G  关联条件 G.MEMBER_CTGRY_ID = H.MEMBER_CTGRY_ID
          *  先整合到表MEMBER_D  在关联BOND_DEAL T
          *CASE WHEN T.BUYER_ID = G.IP_ID THEN H.INS_SHOW_NAME ELSE NULL END
          *CASE WHEN T.SELLER_ID = G.IP_ID THEN H.INS_SHOW_NAME ELSE NULL END
          */
      val after_hours_bond_type_data = sc.textFile(path7)
         .filter(_.length != 0).filter(!_.contains("AFTER"))
         .filter(line => {
           val lineArray =Utils.splite(line)
           if (lineArray.size()!= 2) {
             false
           } else {
             true
           }
         })
         .map(line => {
           val tmp = Utils.splite(line)
           (tmp.get(1), tmp.get(0))
         })
         .collectAsMap()
       val broadAFTER_HOURS_BOND_TYPE = sc.broadcast(after_hours_bond_type_data)
       println("file 7 AFTER_HOURS_BOND_TYPE read succeed! read "+after_hours_bond_type_data.size+" records")

       /**
         * MEMBER_D
         * ip_id	member_ctgry_id	member_cd	full_nm	rt_member_cd	rt_member_full_nm	efctv_from_dt	efctv_to_dt	UNQ_ID_IN_SRC_SYS
         * 关联表BOND_DEAL  T
         * 关联条件：T.BUYER_ID = G.IP_ID
         */
       // val mem_cannot_find__ctgry_id = sc.longAccumulator("mem_cannot_find_CTGRY_ID")
       val mem_cannot_find__after_id = sc.longAccumulator("mem_cannot_find_AFTER_ID")
       val member_d_data = sc.textFile(path8)
         .filter(_.length != 0).filter(!_.contains("MEMBER"))
         .filter(line => {
           val lineArray = Utils.splite(line)
           if (lineArray.size() != 9) {
             false
           } else {
             true
           }
         })
         .map(line => {
           val tmp = Utils.splite(line)
           val data = broadAFTER_HOURS_BOND_TYPE.value
           var ins_show_name="None"
           if(!data.contains(tmp.get(1))){
          //   log.error(" AFTER_HOURS_BOND_TYPE does not contain after_id " + tmp.get(1))
             mem_cannot_find__after_id.add(1l)
           }else{
             ins_show_name = data.get(tmp.get(1)).get
           }
           val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
           val startDate = oracleDateFormat.parse(tmp.get(6))
           val stopDate=oracleDateFormat.parse(tmp.get(7))
           val dates = Set(Tuple2(startDate,stopDate))

           ((tmp.get(0)),(ins_show_name,dates))
         })
         .reduceByKey((x,y)=>(x._1,x._2++y._2))
         .collectAsMap()
        // .collect.foreach(println)
       val broadMEMBER_D = sc.broadcast(member_d_data)
       println("file 8 MEMBER_D read succeed! read "+member_d_data.size+" records")

       /**
         * SOR.BASE_BENM_T_BONDINFO
         * CODE	BONDID
         * code=bond_d.bond_cd   BONDID= BASE_L_BONDSFINALVALUATION.bondid
         * 根据bond_cd得到code  通过code得到bondid  关联表base。。得到VALUATIONTIME
         */
      /*   val mem_cannot_find__bond_id = sc.longAccumulator("mem_cannot_find_BOND_ID")
       val base_benm_t_data = sc.textFile(path9)
         .filter(_.length != 0).filter(!_.contains("BASE"))
         .filter(line => {
           val lineArray = Utils.splite(line)
           if (lineArray.size() != 2) {
             false
           } else {
             true
           }
         })
           .filter(line => {
             val tmp = Utils.splite(line)
             broadBondD.value.get(tmp.get(0))!=None    //过滤掉匹配不到的    待定
           })
         .map(line => {
         val tmp = Utils.splite(line)
         val bond_id=broadBondD.value.get(tmp.get(0)).get._1
         (tmp.get(1),bond_id)
       })
         .collectAsMap()
       val broadBASE_BENM_T = sc.broadcast(base_benm_t_data)
       println("file 9 BASE_BENM_T_BONDINFO read succeed! read "+base_benm_t_data.size+" records")

       /**
         * BASE_L_BONDSFINALVALUATION   D
         * MDFD_DRTN	VLTN_CNVXTY	VFULLPRICE	VALUATIONTIME	BONDID
         * 关联表BOND_DEAL  T
         * 关联条件：T.DEAL_DT = D.VALUATIONTIME(+)    T.  = D.BONDID(+)
         * 临时表1取字段："MDFD_DRTN","VLTN_CNVXTY"
         * 临时表2取字段：VFULLPRICE
         */

       val base_l_bond_data = sc.textFile(path10)
         .filter(_.length != 0).filter(!_.contains("BASE"))
         .filter(line => {
           val lineArray = Utils.splite(line)
           if (lineArray.size() != 5) {
             false
           } else {
             true
           }
         }).filter(line=>{
         val tmp = Utils.splite(line)
         broadBASE_BENM_T.value.get(tmp.get(4))!=None    //过滤掉关联不上bondid的
       })
         .map(line => {
           val tmp = Utils.splite(line)
           val BASE_BENM_T=broadBASE_BENM_T.value
           val bond_id=BASE_BENM_T.get(tmp.get(4)).get
           //时间格式转换 TRUNC（VALUATIONTIME）
           ((bond_id,tmp.get(3)),(tmp.get(0),tmp.get(1),tmp.get(2)))
         })
         .collectAsMap()
       val broadBASE_L_BOND = sc.broadcast(base_l_bond_data)
       println("file 10 BASE_L_BONDSFINALVALUATION read succeed! read "+base_l_bond_data.size+" records")
*/
       /**
         * dpa.trdng_mthd_d
         * trdng_mthd_id  trdng_mthd_blngl_nm
         */
       val trdng_mthd_d_data=sc.textFile(path11)
         .filter(_.length != 0).filter(!_.contains("trdng"))
         .filter(line => {
           val lineArray =Utils.splite(line)
           if (lineArray.size() != 2) {
             false
           } else {
             true
           }
         })
         .filter(line=>{
           val lineArray =Utils.splite(line)
           lineArray.get(1).contains("匿名点击")
         })
         .map(line => {
           val tmp = Utils.splite(line)
           ((tmp.get(0)),(tmp.get(1)))
         })
         .collectAsMap()
       val broadTRDNG_MTHD_D = sc.broadcast(trdng_mthd_d_data)
       println("file 11 trdng_mthd_d read succeed! read "+trdng_mthd_d_data.size+" records")

       /**
         * 临时表一：将各个表的数据关联到表bond_deal中（rdd为以时间，id，成交id 为key  即每条交易为一个rdd  多次用到 cache）
         * BOND_DEAL  B
         * "ST","TRDNG_RI_ID","DIRTY_AMNT","YR_OF_TIME_TO_MRTY","DEAL_DT","DIRTY_PRICE","BOND_DEAL_ID","BUYER_ID","SELLER_ID","YTM_RATE","DEAL_NMBR","TTL_FACE_VALUE",INSTMT_CRNCY
         * BOND_DEAL_ID	BUYER_ID	SELLER_ID	DEAL_NMBR	DEAL_DT	ST	TRDNG_RI_ID	INSTMT_CRNCY	TRADE_AMNT	DIRTY_AMNT	TTL_FACE_VALUE 10	YR_OF_TIME_TO_MRTY	DIRTY_PRICE	YTM_RATE	 trdng_mthd_id
         * 0,1,2,3, 4,5, 6,7,8,9, 10,11,12
         * 5,6,9,11,4,12,0,1,2,13,3 ,10,7
         * 将所用字段都整合到表BOND_DEAL中  临时表2中部分字段从该表中取并进行处理
         * 过滤条件：st<>2   关联表TRDX_DEAL_INFRMN_RMV_D C  C.DIR_OPRTN_TYPE_INDC = 2 AND C.DIR_RMV_STATUS_INDC = 1 AND C.DEAL_ID = BOND_DEAL.BOND_DEAL_ID(保留)
         * 字段处理：CASE WHEN B.YR_OF_TIME_TO_MRTY <= 1 THEN '一年以下(包括一年)'
                   WHEN B.YR_OF_TIME_TO_MRTY <= 3 AND B.YR_OF_TIME_TO_MRTY > 1 THEN '一年至三年(包括三年)'
                   WHEN B.YR_OF_TIME_TO_MRTY <= 5  AND B.YR_OF_TIME_TO_MRTY > 3 THEN '三年至五年(包括五年)'
                  WHEN B.YR_OF_TIME_TO_MRTY <= 7  AND B.YR_OF_TIME_TO_MRTY > 5 THEN '五年至七年(包括七年)'
                  WHEN B.YR_OF_TIME_TO_MRTY <= 10  AND B.YR_OF_TIME_TO_MRTY > 7 THEN '七年至十年(包括十年)' ELSE '十年以上'  END  AS SCND_BLNGL_DESC2
               BOND_D Z        --可写在表BOND_D中
               Z.BOND_CTGRY_NM IN ('国债','政策性金融债','同业存单','短期融资券','超短期融资券','地方政府债','中期票据') THEN Z.BOND_CTGRY_NM ELSE '其他' END
       关联条件：关联上面的表替换相应字段
         */
       val mem_cannot_find_DPS_V_ALL = sc.longAccumulator("mem_cannot_find_DPS_V_ALL")
       val mem_cannot_find_MEMBER_D = sc.longAccumulator("mem_cannot_find_MEMBER_D")
       val bond_deal_data = sc.textFile(path12)
         .filter(_.length != 0).filter(!_.contains("BOND"))
         .filter(line => {
           val lineArray =Utils.splite(line)
           if (lineArray.size() != 15) {
             false
           } else {
             true
           }
         })
         .filter(line=>{
           val lineArray =Utils.splite(line)     //过滤掉st<>2
           if(lineArray.get(5)==2){
             false
           }else{
             true
           }
         })
         .filter(line=>{
           val lineArray =Utils.splite(line)
           if(lineArray.get(7).contains("CNY") || lineArray.get(7).contains("-")){    //保留INSTMT_CRNCY in CNTY or  —-
             true
           }else{
             false
           }
         })
         .filter(line=>{
           val lineArray =Utils.splite(line)
           val data=broadTRDX_DEAL_INFRMN.value
           val result=data.getOrElse(lineArray.get(0),0)   //BOND_DEAL_ID  关联表TRDX_DEAL  能取到值的都过滤掉
           if(result!=0){
             false
           }else{
             true
           }
         })
       /*  .filter(line=>{
           val tmp=Utils.splite(line)
           broadMEMBER_D.value.get(tmp.get(1))!=None  || broadMEMBER_D.value.get(tmp(2))!=None
         })*/
         .filter(line=>{
         val lineArray =Utils.splite(line)
         broadBondD.value.get(lineArray.get(6))!=None    //过滤掉从bond_d表中取不到的债券（代议）
       })
         .map(line=>{
           val tmp=Utils.splite(line)
           val DPS_data=broadDPS_V_ALL.value     //区分交易类型
           //策略类型   deal_deal_idnty=deal_nmbr
           var DEAL_STRATEGY="None"
           if(!DPS_data.contains(tmp.get(3))) {
             log.error("broadDPS_V_ALL does not contain deal_deal_idnty " + tmp.get(3))
             mem_cannot_find_DPS_V_ALL.add(1l)
           }else{
             DEAL_STRATEGY=DPS_data.get(tmp.get(3)).get
           }
           val oracleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
           val deal_dt = oracleDateFormat.parse(tmp.get(4))
         //机构类型 buyer_id   、seller_id   =ip_id
           // val DEAL_STRATEGY=DPS_data.get(tmp(3)).get
           var BUYER_TYPE= "None"
           var SELLER_TYPE="None"
           val MEMBER_data=broadMEMBER_D.value
         if(!MEMBER_data.contains(tmp.get(1))) {
           log.error("MEMBER_D does not contain buyer ip_id " + tmp.get(1))
           mem_cannot_find_MEMBER_D.add(1l)
         }else{
           BUYER_TYPE=MEMBER_data.get(tmp.get(1)).get._1    //需取有效时间段内的？（因ip_id对应的机构类型固定，可不用？）
         }
         if(!MEMBER_data.contains(tmp.get(2))) {
           log.error("MEMBER_D does not contain seller ip_id " + tmp.get(2))
           mem_cannot_find_MEMBER_D.add(1l)
         }else{
           SELLER_TYPE=MEMBER_data.get(tmp.get(2)).get._1
         }
           //基础信息bond_d
           val BOND_data=broadBondD.value
           val bond_tmp=BOND_data.get(tmp.get(6)).get

           var SCND_BLNGL_DESC2="None"
           val scnd_tmp:BigDecimal=BigDecimal(tmp.get(11))
           if(scnd_tmp<=1){
             SCND_BLNGL_DESC2="一年以下(包括一年)"
           }else if(scnd_tmp<=3){
             SCND_BLNGL_DESC2="一年至三年(包括三年)"
           }else if(scnd_tmp<=5){
             SCND_BLNGL_DESC2="三年至五年(包括五年)"
           }else if(scnd_tmp<=7){
             SCND_BLNGL_DESC2="五年至七年(包括七年)"
           }else if(scnd_tmp<=10){
             SCND_BLNGL_DESC2="七年至十年(包括十年)"
           }else{
             SCND_BLNGL_DESC2="十年以上"
           }
           ((tmp.get(6),deal_dt),(bond_tmp,tmp.get(9),tmp.get(11),tmp.get(12),tmp.get(0),tmp.get(1),tmp.get(2),tmp.get(13),tmp.get(3),tmp.get(10),DEAL_STRATEGY,BUYER_TYPE,SELLER_TYPE
             ,SCND_BLNGL_DESC2,tmp.get(14),tmp.get(8)))
         })
          //.collect.foreach(println)
         .cache()
       //.reduceByKey((x,y)=>x)    //临时表一
       println("file 12 bond_deal read succeed! read "+bond_deal_data.count()+" records")

       /**
         * 表XBOND   筛选出匿名点击的债券代码及名称  bond_cd  bond_nm   仅用到临时表一
         * trdng_mthd_id =trdng_mthd_id
         */
      val deal_trdng=bond_deal_data
         .filter(tmp=>{
           broadTRDNG_MTHD_D.value.get(tmp._2._15)!=None
         })
         .map(tmp=>{
           ((tmp._2._1._2),(tmp._2._1._1))
         }).reduceByKey((x,y)=>x)
         .map(tmp=>{
           (tmp._1,tmp._2)
         })
        //.collect.foreach(println)
        .saveToPhoenix("XBOND_INFO",Seq("BOND_CD","BOND_NM"),conf,Some(ZOOKEEPER_URL))
       /**
         * 临时表2的关联条件
         * ON T1.DEAL_DT = T2.DEAL_DT AND T1.TRDNG_RI_ID =T2.TRDNG_RI_ID AND T1.BUYER_ID = T2.SELLER_ID（根据买卖方id拆分成两个RDD进行 full join）
         */
       /**
         * 分开计算求和   全价和券面(买)
         * 根据BUYER_ID进行筛选tmp._2._6（买方）
         * NVL(SUM(T.DIRTY_AMNT),0)
         * NVL(SUM(T.TTL_FACE_VALUE),0)
         */
       val bond_deal_tmp_buy=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._6==null){
              false
            }else{
              true
            }
          })
          .map(
            tmp=>{
              val dirty_amnt=BigDecimal.apply(tmp._2._2)
              val TTL_FACE_VALUE=BigDecimal.apply(tmp._2._10)
              ((tmp._1._1,tmp._1._2,tmp._2._6),(dirty_amnt,TTL_FACE_VALUE))
            }
          ).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))   //1.全价总额（买）  2.劵面总额（买）
          .collectAsMap()
        val broadBUY_ALL=sc.broadcast(bond_deal_tmp_buy)
        /**
          * 分开计算求和   全价和券面(卖)
          * 根据BUYER_ID进行筛选tmp._2._7（卖方）
          * NVL(SUM(T.DIRTY_AMNT),0)
          * NVL(SUM(T.TTL_FACE_VALUE),0)
          */
        val bond_deal_tmp_sell=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._7==null){
              false
            }else{
              true
            }
          })
          .map(
            tmp=>{
              val dirty_amnt=BigDecimal.apply(tmp._2._2)
              val TTL_FACE_VALUE=BigDecimal.apply(tmp._2._10)
              ((tmp._1._1,tmp._1._2,tmp._2._7),(dirty_amnt,TTL_FACE_VALUE))
            }
          ).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))   //1.全价总额（卖）  2.劵面总额（卖）
          .collectAsMap()
        val broadSELL_ALL=sc.broadcast(bond_deal_tmp_sell)
        /**
          *债券收益率-清洗前-买入方
          * 清洗前为全部的数据
          * 计算方法：SUM(T.YTM_RATE*T.DIRTY_AMNT)/SUM(T.DIRTY_AMNT)     ??
          * CASE WHEN NVL(SUM(CASE WHEN G.DEAL_STRATEGY IN ('1','2','3','5','6','7','8','9','10','11') THEN T.DIRTY_AMNT ELSE 0 END),0) <> 0 THEN
          NVL(SUM(CASE WHEN G.DEAL_STRATEGY IN ('1','2','3','5','6','7','8','9','10','11') THEN T.YTM_RATE*T.DIRTY_AMNT ELSE 0 END),0) /
          NVL(SUM(CASE WHEN G.DEAL_STRATEGY IN ('1','2','3','5','6','7','8','9','10','11') THEN T.DIRTY_AMNT ELSE 0 END),0)
          ELSE 0 END BUY_YTM_RATE_BF
          */
        val BUY_YTM_RATE_BF=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._6==null){
              false
            }else{
              true
            }
          })
          .map{
            tmp=>{
              val trade_amnt=BigDecimal.apply(tmp._2._16)
              val ytm_rate=BigDecimal.apply(tmp._2._8)
              ((tmp._1._1,tmp._1._2,tmp._2._6),(trade_amnt,ytm_rate*trade_amnt))
            }
          }
          .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
          .filter(tmp=>{
            tmp._2._1!=0
          })
          .reduceByKey((x,y)=>(x._1,x._2/x._1))
          .map(tmp=>{
            ((tmp._1._1,tmp._1._2,tmp._1._3),tmp._2._2)
          })
          .collectAsMap()
        val broadBUY_YTM_RATE_BF=sc.broadcast(BUY_YTM_RATE_BF)

        /**
          *债券收益率-清洗后-买入方
          * 清洗后为正常交易的数据
          * 计算方法：SUM(T.YTM_RATE*T.DIRTY_AMNT)/SUM(T.DIRTY_AMNT)
          * CASE WHEN NVL(SUM(CASE WHEN G.DEAL_STRATEGY IN ('1','2','3','5','6','7','8','9','10','11') THEN 0 ELSE T.DIRTY_AMNT END),0) <> 0 THEN
          NVL(SUM(CASE WHEN G.DEAL_STRATEGY IN ('1','2','3','5','6','7','8','9','10','11') THEN 0 ELSE T.YTM_RATE*T.DIRTY_AMNT END),0) /
          NVL(SUM(CASE WHEN G.DEAL_STRATEGY IN ('1','2','3','5','6','7','8','9','10','11') THEN 0 ELSE T.DIRTY_AMNT END),0)
          ELSE 0 BUY_YTM_RATE_AF
          */
        val BUY_YTM_RATE_AF=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._6==null){
              false
            }else{
              true
            }
          })
          .filter(tmp=>{
            if(tmp._2._11.equals("正常")){
              true
            }else{false}
          })
          .map{
            tmp=>{
              val trade_amnt=BigDecimal.apply(tmp._2._16)
              val ytm_rate=BigDecimal.apply(tmp._2._8)
              ((tmp._1._1,tmp._1._2,tmp._2._6),(trade_amnt,ytm_rate*trade_amnt))
            }
          }
          .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
          .filter(tmp=>{
            tmp._2._1!=0
          })
          .reduceByKey((x,y)=>(x._1,x._2/x._1))
          .map(tmp=>{
            ((tmp._1._1,tmp._1._2,tmp._1._3),tmp._2._2)
          })
          .collectAsMap()
        val broadBUY_YTM_RATE_AF=sc.broadcast(BUY_YTM_RATE_AF)
        /**
          *债券收益率-清洗前-卖出方
          * 计算方法同买入
          * 过滤条件：tmp._2._7(卖出id不为空)
          */
        val SELL_YTM_RATE_BF=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._7==null){
              false
            }else{
              true
            }
          })
          .map{
            tmp=>{
              val trade_amnt=BigDecimal.apply(tmp._2._16)
              val ytm_rate=BigDecimal.apply(tmp._2._8)
              ((tmp._1._1,tmp._1._2,tmp._2._7),(trade_amnt,ytm_rate*trade_amnt))
            }
          }
          .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
          .filter(tmp=>{
            tmp._2._1!=0
          })
          .reduceByKey((x,y)=>(x._1,x._2/x._1))
          .map(tmp=>{
            ((tmp._1._1,tmp._1._2,tmp._1._3),tmp._2._2)
          })
          .collectAsMap()
        val broadSELL_YTM_RATE_BF=sc.broadcast(SELL_YTM_RATE_BF)
        //.reduceByKey((x,y),(x._1+y._1,x._2/y._2))

        /**
          *债券收益率-清洗后-卖出方
          * 计算方法同买入
          * 过滤条件：tmp._2._7(卖出id不为空)
          */
        val SELL_YTM_RATE_AF=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._7==null){
              false
            }else{
              true
            }
          })
          .filter(tmp=>{
            if(tmp._2._11.equals("正常")){
              true
            }else{false}
          })
          .map{
            tmp=>{
              val trade_amnt=BigDecimal.apply(tmp._2._16)
              val ytm_rate=BigDecimal.apply(tmp._2._8)
              ((tmp._1._1,tmp._1._2,tmp._2._7),(trade_amnt,ytm_rate*trade_amnt))
            }
          }
          .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
          .filter(tmp=>{
            tmp._2._1!=0
          })
          .reduceByKey((x,y)=>(x._1,x._2/x._1))
          .map(tmp=>{
            ((tmp._1._1,tmp._1._2,tmp._1._3),tmp._2._2)
          })
          .collectAsMap()
        val broadSELL_YTM_RATE_AF=sc.broadcast(SELL_YTM_RATE_AF)

        /**
          * 临时表2
          * key: 债券id 、交易日期
          * 字段：
          * BOND_D : BOND_ID BOND_CD BOND_NM   BOND_CTGRY_ID FRST_VALUE_DT MRTY_DT  TTL_FACE_VALUE
          * ORG_ID BUY_ACT_ALL BUY_PAPER_ALL SELL_ACT_ALL SELL_PAPER_ALL VFULLPRICE INS_SHOW_NAME BUY_YTM_RATE_BF
          * BUY_YTM_RATE_AF SELL_YTM_RATE_BF SELL_YTM_RATE_AF dirty_price_nume_bf dirty_price_deno_bf dirty_price_nume_af dirty_price_deno_af
          */
      /*  val bond_deal_tmp=bond_deal_data.map(
          tmp=>{
            ((tmp._1._1,tmp._1._2,tmp._2._6),(tmp._2._16))
          }).reduceByKey((x,y)=>x)
          .filter(tmp=>{
            broadBondD.value.get(tmp._1._1).get!=None
          })
          .map(tmp=>{
            var org_id=tmp._1._3
            val data_sell=broadSELL_ALL.value
            /*if(!data_sell.keysIterator.contains(org_id)){
             org_id= data_sell.keysIterator.foreach(_._3).toString
            }*/
            var BUY_ACT_ALL:BigDecimal=BigDecimal(0)
            var BUY_PAPER_ALL:BigDecimal=BigDecimal(0)
            var SELL_ACT_ALL:BigDecimal=BigDecimal(0)
            var SELL_PAPER_ALL:BigDecimal=BigDecimal(0)
            var INS_SHOW_NAME="None"
            var BUY_YTM_RATE_BF:BigDecimal=BigDecimal(0)
            var BUY_YTM_RATE_AF:BigDecimal=BigDecimal(0)
            var SELL_YTM_RATE_BF:BigDecimal=BigDecimal(0)
            var SELL_YTM_RATE_AF:BigDecimal=BigDecimal(0)
           
              if(broadBUY_ALL.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
                val data_buy_all=broadBUY_ALL.value.get((tmp._1._1,tmp._1._2,org_id)).get
                BUY_ACT_ALL=data_buy_all._1
                BUY_PAPER_ALL=data_buy_all._2
              }else{
                false;
              }
            if(broadSELL_ALL.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
              val data_sell_all=broadSELL_ALL.value.get((tmp._1._1,tmp._1._2,org_id)).get
               SELL_ACT_ALL=data_sell_all._1
               SELL_PAPER_ALL=data_sell_all._2
            }
            if(broadMEMBER_D.value.get(org_id)!=None){
              val dataDate=broadMEMBER_D.value.get(org_id).get._2
              for (elem <- dataDate) {
                if(tmp._1._2.after(elem._1) && tmp._1._2.before(elem._2)){
                  INS_SHOW_NAME=broadMEMBER_D.value.get(org_id).get._1
                }
              }
            }
            if(broadBUY_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){

              BUY_YTM_RATE_BF=broadBUY_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id)).get
            }
            if(broadBUY_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
              val data_buy_ytm_af=broadBUY_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id)).get
              BUY_YTM_RATE_AF=data_buy_ytm_af
            }
            if(broadSELL_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
              val data_sell_ytm_bf=broadSELL_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id)).get
              SELL_YTM_RATE_BF=data_sell_ytm_bf
            }
            if(broadSELL_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
              val data_sell_ytm_af=broadSELL_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id)).get
              SELL_YTM_RATE_AF=data_sell_ytm_af
            }
              val data_bond_d=broadBondD.value.get(tmp._1._1).get
            val bond_cd=data_bond_d._2
            val bond_nm=data_bond_d._1
            val bond_ctgry_id=data_bond_d._3
            val frst_value_dt=data_bond_d._5
            val mrty_dt =data_bond_d._6
            val ttl_face_value=data_bond_d._8

            ((tmp._1._1,tmp._1._2,org_id),(bond_cd,bond_nm,bond_ctgry_id,frst_value_dt,mrty_dt,ttl_face_value,
              BUY_ACT_ALL,BUY_PAPER_ALL ,SELL_ACT_ALL ,SELL_PAPER_ALL,INS_SHOW_NAME, BUY_YTM_RATE_BF,
              BUY_YTM_RATE_AF, SELL_YTM_RATE_BF, SELL_YTM_RATE_AF))
          })
          .collectAsMap()
          val broadBond_deal_tmp1=sc.broadcast(bond_deal_tmp)*/
          //.cache()
          //.collect.foreach(println)

        val bond_deal_tmp2=bond_deal_data
          .filter(tmp=>{
            tmp._2._7!=None  || tmp._2._7!=null
          })
          .map(
                  tmp=>{
                    ((tmp._1._1,tmp._1._2,tmp._2._7),(tmp._2._16))
                  }).reduceByKey((x,y)=>x)
                  .filter(tmp=>{
                    broadBondD.value.get(tmp._1._1).get!=None
                  })
                  .map(tmp=>{
                    val org_id=tmp._1._3
                    var SELL_ACT_ALL:BigDecimal=BigDecimal(0)
                    var SELL_PAPER_ALL:BigDecimal=BigDecimal(0)
                    var SELL_YTM_RATE_BF:BigDecimal=BigDecimal(0)
                    var SELL_YTM_RATE_AF:BigDecimal=BigDecimal(0)

                    if(broadSELL_ALL.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
                      val data_sell_all=broadSELL_ALL.value.get((tmp._1._1,tmp._1._2,org_id)).get
                       SELL_ACT_ALL=data_sell_all._1
                       SELL_PAPER_ALL=data_sell_all._2
                    }

                    if(broadSELL_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
                      val data_sell_ytm_bf=broadSELL_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id)).get
                      SELL_YTM_RATE_BF=data_sell_ytm_bf
                    }
                    if(broadSELL_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
                      val data_sell_ytm_af=broadSELL_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id)).get
                      SELL_YTM_RATE_AF=data_sell_ytm_af
                    }

                    ((tmp._1._1,tmp._1._2,org_id),(SELL_ACT_ALL ,SELL_PAPER_ALL,
                      SELL_YTM_RATE_BF, SELL_YTM_RATE_AF))
                  })


          val bond_deal_tmp1=bond_deal_data
              .filter(tmp=>{
                tmp._2._6!=None || tmp._2._6!=null
              })
            .map(
                   tmp=>{
                     ((tmp._1._1,tmp._1._2,tmp._2._6),(tmp._2._16))
                   }).reduceByKey((x,y)=>x)
                   .filter(tmp=>{
                     broadBondD.value.get(tmp._1._1).get!=None
                   })
                   .map(tmp=>{
                     var org_id=tmp._1._3

                     var BUY_ACT_ALL:BigDecimal=BigDecimal(0)
                     var BUY_PAPER_ALL:BigDecimal=BigDecimal(0)
                     var BUY_YTM_RATE_BF:BigDecimal=BigDecimal(0)
                     var BUY_YTM_RATE_AF:BigDecimal=BigDecimal(0)


                       if(broadBUY_ALL.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
                         val data_buy_all=broadBUY_ALL.value.get((tmp._1._1,tmp._1._2,org_id)).get
                         BUY_ACT_ALL=data_buy_all._1
                         BUY_PAPER_ALL=data_buy_all._2
                       }else{
                         false;
                       }

                     if(broadBUY_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){

                       BUY_YTM_RATE_BF=broadBUY_YTM_RATE_BF.value.get((tmp._1._1,tmp._1._2,org_id)).get
                     }
                     if(broadBUY_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id))!=None){
                       val data_buy_ytm_af=broadBUY_YTM_RATE_AF.value.get((tmp._1._1,tmp._1._2,org_id)).get
                       BUY_YTM_RATE_AF=data_buy_ytm_af
                     }
                     ((tmp._1._1,tmp._1._2,org_id),(BUY_ACT_ALL,BUY_PAPER_ALL , BUY_YTM_RATE_BF,
                       BUY_YTM_RATE_AF))
                   })


         val bond_deal_tmp=bond_deal_tmp1.fullOuterJoin(bond_deal_tmp2)
        .map(tmp=>{
          var BUY_ACT_ALL:BigDecimal=BigDecimal(0)
           var BUY_PAPER_ALL:BigDecimal=BigDecimal(0)
           var BUY_YTM_RATE_BF:BigDecimal=BigDecimal(0)
           var BUY_YTM_RATE_AF:BigDecimal=BigDecimal(0)
        if(tmp._2._1!=None){
            BUY_ACT_ALL=tmp._2._1.get._1
            BUY_PAPER_ALL=tmp._2._1.get._2
            BUY_YTM_RATE_BF=tmp._2._1.get._3
            BUY_YTM_RATE_AF=tmp._2._1.get._4

        }
           var SELL_ACT_ALL:BigDecimal=BigDecimal(0)
          var SELL_PAPER_ALL:BigDecimal=BigDecimal(0)
          var SELL_YTM_RATE_BF:BigDecimal=BigDecimal(0)
          var SELL_YTM_RATE_AF:BigDecimal=BigDecimal(0)
          if(tmp._2._2!=None){
             SELL_ACT_ALL=tmp._2._2.get._1
             SELL_PAPER_ALL=tmp._2._2.get._2
             SELL_YTM_RATE_BF=tmp._2._2.get._3
             SELL_YTM_RATE_AF=tmp._2._2.get._4
                  }
          
                      var INS_SHOW_NAME="None"
          if(broadMEMBER_D.value.get(tmp._1._3)!=None){
            val dataDate=broadMEMBER_D.value.get(tmp._1._3).get._2
            for (elem <- dataDate) {
              if(tmp._1._2.after(elem._1) && tmp._1._2.before(elem._2)){
                INS_SHOW_NAME=broadMEMBER_D.value.get(tmp._1._3).get._1
              }
            }
          }
           val data_bond_d=broadBondD.value.get(tmp._1._1).get
            val bond_cd=data_bond_d._2
            val bond_nm=data_bond_d._1
            val bond_ctgry_id=data_bond_d._3
            val frst_value_dt=data_bond_d._5
            val mrty_dt =data_bond_d._6
            val ttl_face_value=data_bond_d._8

          ((tmp._1._1,tmp._1._2,tmp._1._3),(bond_cd,bond_nm,bond_ctgry_id,frst_value_dt,mrty_dt,ttl_face_value,
            BUY_ACT_ALL,BUY_PAPER_ALL ,SELL_ACT_ALL ,SELL_PAPER_ALL,INS_SHOW_NAME, BUY_YTM_RATE_BF,
            BUY_YTM_RATE_AF, SELL_YTM_RATE_BF, SELL_YTM_RATE_AF
          ))                  
        })

         //  .distinct()
            .cache()
           //.collect.foreach(println)

        /**
          * 计算每天的成交量和每日换手率分子  SUM(DIRTY_AMNT)   SUM(TTL_FACE_VALUE)
          */
         val BOND_BSC_INFO_tmp1=bond_deal_data.map(tmp=>{
           ((tmp._1._1,tmp._1._2),(tmp._2._2.toDouble,tmp._2._10.toDouble,tmp._2._1._9.toDouble))
         })
           .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3))
          .reduceByKey((x,y)=>(x._1,x._2/x._3,x._3))
          .map(tmp=>{
            ((tmp._1._1,tmp._1._2),(tmp._2._1,tmp._2._2))
          })
           .collectAsMap()
         val broadBOND_BSC_INFO_tmp1=sc.broadcast(BOND_BSC_INFO_tmp1)

         /**
           * 计算成交笔数COUNT(DISTINCT ...)
           */
         val BOND_BSC_INFO_tmp2=bond_deal_data.map(tmp=>{
           ((tmp._1._1,tmp._1._2,tmp._2._5),1)
         })
           .reduceByKey((x,y)=>x+y)
           .map(tmp=>{
             ((tmp._1._1,tmp._1._2),tmp._2)
           })//待定
           .collectAsMap()
         val broadBOND_BSC_INFO_tmp2=sc.broadcast(BOND_BSC_INFO_tmp2)
         /**
           * BOND_BSC_INFO
           */
           /*val BOND_BSC_INFO_tmp=bond_deal_data
             .filter(tmp=>{
               tmp._2._1._10!=None  && tmp._2._1._5!=None  && tmp._2._1._6!=None  && tmp._2._1._9!=None  //去除没有起息日到期日 及分母为空的字段
             })
             .map(tmp=>{
               val data_tmp1=broadBOND_BSC_INFO_tmp1.value.get((tmp._1._1,tmp._1._2)).get
               val TRD_AMNT=data_tmp1._1
               val s=data_tmp1._2.toDouble
               val DAY_TURNOVER_RATE=s/tmp._2._1._9.toDouble
               val data_tmp2=broadBOND_BSC_INFO_tmp2.value
               val DEAL_CNT=data_tmp2.get((tmp._1._1,tmp._1._2)).get  //获取成交笔数
               val issue_vlmn= tmp._2._1._9.toDouble/100000000
             })*/

       val BOND_BSC_INFO=bond_deal_data
           .filter(tmp=>{
             tmp._2._1._10!=None  && tmp._2._1._5!=None  && tmp._2._1._6!=None  && tmp._2._1._9!=None  //去除没有起息日到期日 及分母为空的字段
           })
         .map(tmp=>{
         val data_tmp1=broadBOND_BSC_INFO_tmp1.value.get((tmp._1._1,tmp._1._2)).get
         val TRD_AMNT=data_tmp1._1
         val DAY_TURNOVER_RATE=data_tmp1._2
         val data_tmp2=broadBOND_BSC_INFO_tmp2.value
         val DEAL_CNT=data_tmp2.get((tmp._1._1,tmp._1._2)).get  //获取成交笔数
         val issue_vlmn= tmp._2._1._9.toDouble/100000000
         val yr_of_time=tmp._2._3

           val oracleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
           val FRST_VALUE_DT= oracleDateFormat.parse(tmp._2._1._5)
           val MRTY_DT=oracleDateFormat.parse(tmp._2._1._6)
           val LSTNG_DT=oracleDateFormat.parse(tmp._2._1._10)
         ((tmp._1._1,tmp._1._2),(tmp._2._1._2,tmp._2._1._1,tmp._2._1._4,BigDecimal.apply(yr_of_time),FRST_VALUE_DT,MRTY_DT,
           tmp._2._1._7,BigDecimal.apply(issue_vlmn),tmp._2._1._8,BigDecimal.apply(TRD_AMNT),BigDecimal.apply(DEAL_CNT),BigDecimal.apply(DAY_TURNOVER_RATE)))
       })
         .reduceByKey((x,y)=>x)
         .map(tmp=>{
           (tmp._1._1,tmp._1._2,tmp._2._1,tmp._2._2,tmp._2._3,tmp._2._4.bigDecimal,tmp._2._5,tmp._2._6,tmp._2._7,tmp._2._8.bigDecimal,
             tmp._2._9,tmp._2._10.bigDecimal,tmp._2._11.bigDecimal,tmp._2._12.bigDecimal)
         })
       // .collect.foreach(println)
      .saveToPhoenix("BOND_BSC_INFO",Seq("BOND_ID","DEAL_DT","BOND_CD","BOND_NM","BOND_CTGRY_NM","YR_OF_TIME_TO_MRTY","FRST_VALUE_DT"
         ,"MRTY_DT","BOND_PRD","ISSUE_VLMN","CSTDN","TRD_AMNT","DEAL_CNT","DAY_TURNOVER_RATE"),conf,Some(ZOOKEEPER_URL))

     /**
       * BOND_TURNOVER_RATE
       * 按照对比债券期限进行区分  （全部）
       * 按天 按单个债券 分债券类型 分债券期限 汇总
       * 个债交易整体表现 日均换手率 公用一张表 BOND_TURNOVER_RATE 取 BOND_PRD=’全部‘ DAY_TURNOVER_RATE 即为每日换手率 算术平均，并排名计算指标值
       * 个债交易整体表现 质押式折扣率 公用 BOND_TURNOVER_RATE 字段 ZY_RATE 为 每日加权质押物折扣率  取记录bond_prd = '全部' 为每日的
       */
     val mem_cannot_find_EV_CLTRL_DTLS = sc.longAccumulator("mem_cannot_find_EV_CLTRL_DTLS")
      val BOND_TURNOVER_RATE_tmp1=bond_deal_data
         .filter(tmp=>{
           tmp._2._1._9!=None  && tmp._2._1._9!="0"
         })
       .map(tmp=>{
       val data_tmp1=broadBOND_BSC_INFO_tmp1.value
       val TRD_AMNT_SUM=data_tmp1.get((tmp._1._1,tmp._1._2)).get._1
       val DAY_TURNOVER_RATE=data_tmp1.get((tmp._1._1,tmp._1._2)).get._2.toDouble/tmp._2._1._9.toDouble
       val BOND_PRD="全部"
       val data_tmp3=broadEV_CLTRL_DTLS.value
       var ZY_RATE:BigDecimal=BigDecimal(0)
       if(data_tmp3.get(tmp._1._1,tmp._1._2)==None){
           log.error("BAND_REPO_DEAL does not contain bond_id  deal_dt " + tmp._1._1+","+tmp._1._2)
         mem_cannot_find_EV_CLTRL_DTLS.add(1l)
       } else{
           ZY_RATE=data_tmp3.get(tmp._1._1,tmp._1._2).get
       }
       ((tmp._1._1,tmp._1._2,tmp._2._1._2,tmp._2._1._1,tmp._2._1._11),(BOND_PRD,BigDecimal.apply(DAY_TURNOVER_RATE),BigDecimal.apply(TRD_AMNT_SUM),ZY_RATE))
     }).reduceByKey((x,y)=>x)
       .map(tmp=>{
         (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._1._5,tmp._2._1,tmp._2._2.bigDecimal,tmp._2._3.bigDecimal,tmp._2._4.bigDecimal)
       })
      // .collect.foreach(println)
       .saveToPhoenix("BOND_TURNOVER_RATE",Seq("BOND_ID","DEAL_DT","BOND_CD","BOND_NM","BOND_CTGRY_NM2","BOND_PRD",
         "DAY_TURNOVER_RATE","TRD_AMNT_SUM","ZY_RATE"),conf,Some(ZOOKEEPER_URL))

     /**
       * BOND_TURNOVER_RATE
       * 按照对比债券期限进行区分  （根据债券期限区分）
       * 按天 按单个债券 分债券类型 分债券期限 汇总
       * 个债交易整体表现 日均换手率 公用一张表 BOND_TURNOVER_RATE 取 BOND_PRD=’全部‘ DAY_TURNOVER_RATE 即为每日换手率 算术平均，并排名计算指标值
       * 个债交易整体表现 质押式折扣率 公用 BOND_TURNOVER_RATE 字段 ZY_RATE 为 每日加权质押物折扣率  取记录bond_prd = '全部' 为每日的
       */
      val BOND_TURNOVER_RATE_tmp2=bond_deal_data.map(tmp=>{
            val data_tmp1=broadBOND_BSC_INFO_tmp1.value
            val TRD_AMNT_SUM=data_tmp1.get((tmp._1._1,tmp._1._2)).get._1
            val DAY_TURNOVER_RATE=data_tmp1.get((tmp._1._1,tmp._1._2)).get._2.toDouble/tmp._2._1._9.toDouble

            ((tmp._1._1,tmp._1._2,tmp._2._1._2,tmp._2._1._1,tmp._2._1._11),(tmp._2._14,BigDecimal.apply(DAY_TURNOVER_RATE),BigDecimal.apply(TRD_AMNT_SUM),null))
          }).reduceByKey((x,y)=>x)
            .map(tmp=>{
              (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._1._5,tmp._2._1,tmp._2._2.bigDecimal,tmp._2._3.bigDecimal,tmp._2._4)
            })
             // .collect.foreach(println)

            .saveToPhoenix("BOND_TURNOVER_RATE",Seq("BOND_ID","DEAL_DT","BOND_CD","BOND_NM","BOND_CTGRY_NM2","BOND_PRD",
             "DAY_TURNOVER_RATE","TRD_AMNT_SUM","ZY_RATE"),conf,Some(ZOOKEEPER_URL))

          /**
            * BOND_INCOME
            */

        val BOND_INCOME_data=bond_deal_tmp.map(tmp=> {
             (tmp._1._2, tmp._1._1, tmp._2._1, tmp._2._2, tmp._1._3,tmp._2._8.bigDecimal, tmp._2._7.bigDecimal, tmp._2._10.bigDecimal, tmp._2._9.bigDecimal)
            })
           // .collect.foreach(println)
             .saveToPhoenix("BOND_INCOME",Seq("DEAL_DT","BOND_ID","BOND_CD","BOND_NM","ORG_ID","BUY_PAPER_ALL",
               "BUY_ACT_ALL","SELL_PAPER_ALL","SELL_ACT_ALL"),conf,Some(ZOOKEEPER_URL))
            //写入hbase
            /**
              * BOND_DEAL_TREND
              * sum(YTM_RATE*DIRTY_AMNT)
              * 过滤条件：清洗前   策略性交易
              * 以全价加权（需更改）
              */
          val YTM_RATE_BF_FM=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._11.equals("正常")){
              false
            }else{true}
          })
          .map{
            tmp=>{
              val DIRTY_AMNT=BigDecimal(tmp._2._2)
              val YTM_RATE=BigDecimal(tmp._2._8)
              ((tmp._1._1,tmp._1._2),(DIRTY_AMNT,YTM_RATE*DIRTY_AMNT))
            }
          }
          .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
           .filter(tmp=>{
             !tmp._2._1.equals("")  &&  !tmp._2._1.equals("0")   && tmp._2._1!=null  && tmp._2._1!=None
           })
           .reduceByKey((x,y)=>(x._1,x._2/x._1))
          .collectAsMap()
        val broadYTM_RATE_BF_FM=sc.broadcast(YTM_RATE_BF_FM)
        /**
          * 清洗后
          */
        val YTM_RATE_AF_FM=bond_deal_data
          .filter(tmp=>{
            if(tmp._2._11.equals("正常")){
              true
            }else{false}
          })
          .map{
            tmp=>{
              val DIRTY_AMNT=BigDecimal(tmp._2._2)
              val YTM_RATE=BigDecimal(tmp._2._8)
              ((tmp._1._1,tmp._1._2),(DIRTY_AMNT,YTM_RATE*DIRTY_AMNT))
            }
          }
          .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
          .filter(tmp=>{
            !tmp._2._1.equals("")  &&  !tmp._2._1.equals("0")   && tmp._2._1!=null  && tmp._2._1!=None
          })
          .reduceByKey((x,y)=>(x._1,x._2/x._1))
          .collectAsMap()
        val broadYTM_RATE_AF_FM=sc.broadcast(YTM_RATE_AF_FM)

        /**
          * 写入到表BOND_DEAL_TREND
          */
        val BOND_DEAL_TREND=bond_deal_data
          .map{
            tmp=> {
              var YTM_RATE_BF:BigDecimal =BigDecimal(0)
              val data_YTM_RATE_BF = broadYTM_RATE_BF_FM.value.get((tmp._1._1, tmp._1._2))
              if (data_YTM_RATE_BF!=None) {
                YTM_RATE_BF = data_YTM_RATE_BF.get._2
              }
              var YTM_RATE_AF:BigDecimal = 0
              if (broadYTM_RATE_AF_FM.value.get((tmp._1._1, tmp._1._2))==None) {
                //  log.error("YTM_RATE_BF_FM does not contain bond_id  deal_dt " + tmp._1._1+","+tmp._1._2)
             //   mem_cannot_find_ev_id.add(1l)

              } else {
              YTM_RATE_AF = broadYTM_RATE_AF_FM.value.get((tmp._1._1, tmp._1._2)).get._2
            }

              ((tmp._1._2,tmp._1._1,tmp._2._1._2,tmp._2._1._1),(YTM_RATE_BF, YTM_RATE_AF))
            }
          }.reduceByKey((x,y)=>x)
          .map(tmp=>{
            (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._2._1.bigDecimal,tmp._2._2.bigDecimal)
          })
        // .collect.foreach(println)
            .saveToPhoenix("BOND_DEAL_TREND",Seq("DEAL_DT","BOND_ID","BOND_CD","BOND_NM","DIRTY_PRICE_WG_BF","DIRTY_PRICE_WG_AF"),conf,Some(ZOOKEEPER_URL))

          /**
            * 写入到表BOND_DEAL_TREND_ORG
            */
        val BOND_DEAL_TREND_ORG=bond_deal_tmp
            .map{
              tmp=>{
                ((tmp._1._2,tmp._1._1,tmp._2._1,tmp._2._2,tmp._1._3,tmp._2._11),(tmp._2._12,tmp._2._13,tmp._2._14,tmp._2._15))
              }
            }.reduceByKey((x,y)=>x)
            .map(tmp=>{
              (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._1._5,tmp._1._6,tmp._2._1.bigDecimal,tmp._2._2.bigDecimal,tmp._2._3.bigDecimal,tmp._2._4.bigDecimal)
            })
         //   .collect.foreach(println)
           .saveToPhoenix("BOND_DEAL_TREND_ORG",Seq("DEAL_DT","BOND_ID","BOND_CD","BOND_NM","ORG_ID","INS_SHOW_NAME",
             "BUY_YTM_RATE_BF","BUY_YTM_RATE_AF","SELL_YTM_RATE_BF","SELL_YTM_RATE_AF"),conf,Some(ZOOKEEPER_URL))

         /**
           * BOND_DEAL_ORGTYPE
           */
       val  BOND_DEAL_ORGTYPE=bond_deal_tmp
         .map {
         (tmp => {
           ((tmp._1._2,tmp._1._1,tmp._2._1,tmp._2._2,tmp._2._11),(tmp._2._7,tmp._2._9,tmp._2._10,tmp._2._8))
         })
       }
         .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4))
         .reduceByKey((x,y)=>(x._1+x._2,x._4-x._3,x._3,x._4))
         .map(tmp=>{
           (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._1._5,tmp._2._1.bigDecimal,tmp._2._2.bigDecimal)
         })
    //    .collect.foreach(println)
         .saveToPhoenix("BOND_DEAL_ORGTYPE",Seq("DEAL_DT","BOND_ID","BOND_CD","BOND_NM","INS_SHOW_NAME","TRD_AMNT","NET_AMNT"),conf,Some(ZOOKEEPER_URL))
       //写入hbase

       /**
         * BOND_DEAL_ORGLIST
         */

        val BOND_DEAL_ORGLIST=bond_deal_tmp.map(tmp=>{
           ((tmp._1._2,tmp._1._1,tmp._2._1,tmp._2._2,tmp._2._3,tmp._1._3,tmp._2._11),1)
         })
           .reduceByKey(_+_)
           .map(tmp=>{
             (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._1._5,tmp._1._6,tmp._1._7)
           })
         //  .collect.foreach(println)
          .saveToPhoenix("BOND_DEAL_ORGLIST",Seq("DEAL_DT","BOND_ID","BOND_CD","BOND_NM","BOND_CTGRY_ID","ORG_ID","INS_SHOW_NAME"),conf,Some(ZOOKEEPER_URL))
         /**
           * BOND_DEAL_SMART
           */
         val BOND_DEAL_SMART=bond_deal_data.map(tmp=>{
           var TRADE_NORMAL=0
           var TRADE_SMART=0
           if(tmp._2._11.equals("正常")){
             TRADE_NORMAL=TRADE_NORMAL+1
           }else{
             TRADE_SMART=TRADE_SMART+1
           }
           ((tmp._1._2,tmp._1._1,tmp._2._1._2,tmp._2._1._1,tmp._2._1._11),(TRADE_NORMAL,TRADE_SMART))
         })
           .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
           .map(tmp=>{
             (tmp._1._1,tmp._1._2,tmp._1._3,tmp._1._4,tmp._1._5,tmp._2._1,tmp._2._2)
           })
          // .collect.foreach(println)
          .saveToPhoenix("BOND_DEAL_SMART",Seq("DEAL_DT","BOND_ID","BOND_CD","BOND_NM","BOND_CTGRY_NM2","TRADE_NORMAL","TRADE_SMART"),conf,Some(ZOOKEEPER_URL))

         sc.stop()
    }

}
