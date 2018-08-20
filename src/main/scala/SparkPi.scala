import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaohou on 10:45 2018/7/11.
  * description:
  */
object SparkPi {
  def main(args: Array[String]) {
      //本地调试用  暂时 报错 Unsupported message RpcMessage
//    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://192.168.186.151:7077")
//      .setJars(List("file:///E:\\tool\\workspace\\DWSpark01\\out\\artifacts\\SparkPi\\dwspark01.jar"))
//    System.setProperty("hadoop.home.dir", "D:/BIGDATA/hadoop-2.7.2")

    //集群环境配置 SparkContext
    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)
    val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
    //开始日期 结束日期
    val beginDate = format.parse(args(0))
    val endDate = format.parse(args(1))
    //机构名称  测试null
    val orgName = args(2)
    //参与者类型   自营 产品 全部
    val playerType = args(3)
    //债券类型：利率债、信用债、同业存单、全部
    val bondType = args(4)
    println(" 入参：beginDate:"+beginDate+",endDate:"+endDate+",orgName:"+orgName+",playerType:"+playerType+",bondType:"+bondType)

    //sh spark-submit --master spark://192.168.186.151:7077 --class SparkPi /home/user/Templates/dwspark01.jar "2018/07/15" "2018/07/23" "机构名称" "全部" "利率债"
    //"2018/07/18" "2018/07/23" "null" "全部" "全部"
    mapTest(sc,beginDate,endDate,orgName,playerType,bondType)

  }

  def mapTest(sc:SparkContext,beginDate:Date,endDate:Date,orgName:String,playerType:String,bondType:String): Unit = {
    println(" my test begin ====================================================================================")
    //读取 现券市场 的成交数据
    val file = sc.textFile("/home/user/Templates/ibo0001.txt", 3)
    //filter操作
    println(" my test：filter操作过滤日期:"+beginDate+"-"+endDate+" ====================================================================================")
    val format = new java.text.SimpleDateFormat("yyyy/MM/dd")

    //满足查询条件的数据
    val filterdemo = file.filter(x => {
      val info = x.split(",")
      (beginDate.compareTo(format.parse(info(0)))<=0)&&
      (format.parse(info(0)).compareTo(endDate)<=0)&&
        (if(null == orgName||orgName.equals(""))true else info(1).equals(orgName))&&
        (if("全部".equals(playerType))true else info(2).equals(playerType))&&
        (if("全部".equals(bondType))true else info(3).equals(bondType))
      //作 持久化 persist()
    })//.persist()

    //时间段内 工作日数量（时间段内-有成交的工作日）
    val workDays = file.filter(x => {
      val info = x.split(",")
      (beginDate.compareTo(format.parse(info(0)))<=0)&&
        (format.parse(info(0)).compareTo(endDate)<=0)
    }).map(x=>{
      val info = x.split(",")
      info(0)
    }).distinct().count()
    println("时间段内-有成交的工作日数量:"+workDays)

    var num = 0
    //环比工作日
    val beforeWork = file.filter(x => {
      val info = x.split(",")
      //查询 开始日期 前的rdd
        format.parse(info(0)).compareTo(beginDate) < 0
    }).map(x=>{
      val info = x.split(",")
      info(0)
    }).map(x=>(x,"")).distinct().sortByKey(false).take(workDays.toInt).foreach(x=>{
      num = 1+num
      println(num +"\t ==================" +x)
    })

    beforeWork


    val beforeWorks = file.filter(x => {
      val info = x.split(",")
      //查询 开始日期 前的rdd
      format.parse(info(0)).compareTo(beginDate) < 0
    }).map(x=>{
      val info = x.split(",")
      info(0)
    }).map(x=>(x,"")).distinct().sortByKey(false).take(workDays.toInt).map(x=>{
      val info = x._1
      info(0)
    })

    //take是一个action，作用是取出前n条数据发送到driver，一般用于开发测试
    filterdemo.take(14).foreach(println)
    //交易规模：成交金额 + all
    val allPrice = filterdemo.map(x=>{
      val info = x.split(",")
      info(4).toInt
    }).reduce((sum,i)=>sum+i)
    println("交易规模："+allPrice.toInt)
    //环比交易规模：

    //成交笔数：count（成交编号）
    val dealNum = filterdemo.map(x=>{
      val info = x.split(",")
      info(5)
    }).count()
    println("成交笔数："+dealNum)

    //成交天数：日期去重 count
    val dealDays = filterdemo.map(x=>{
      val info = x.split(",")
      info(0)
    }).distinct().count()
    println("成交天数："+dealDays)

    //交易对手数：机构名称去重 count
    val dealerNum = filterdemo.map(x=>{
      val info = x.split(",")
      info(1)
    }).distinct().count()
    println("交易对手数："+dealerNum)

    //交易债券数: 交易债券去重
    val bondNum = filterdemo.map(x=>{
      val info = x.split(",")
      info(7)
    }).distinct().count()
    println("交易债券数:"+bondNum)

    println(" my test end ====================================================================================")
  }
  class test{

  }
}
