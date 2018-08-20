import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by yaohou on 22:10 2018/7/10.
  * description: 
  */
object WordCount {
  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("wordcount").setMaster("spark://192.168.186.151:7077")
      //.setJars(List("E:\\tool\\workspace\\DWSpark01\\out\\artifacts\\dwspark01\\dwspark01.jar"))
    val conf=new SparkConf().setAppName("wordcount")
    //初始化SparkContext - sc.
    val sc=new SparkContext(conf)
    //第一个spark 程序，计算出现的词语的个数
    //hellogll(sc)
    //读取文件中的数据，到map中，
   // mapTest(sc)


//    val slices = if (args.length > 0) args(0).toInt else 2
//    val n = 100000 * slices
//    val count = spark.parallelize(1 to n, slices).map { i =>
//      val x = Math.random * 2 - 1
//      val y = Math.random * 2 - 1
//      if (x * x + y * y < 1) 1 else 0
//    }.reduce(_ + _)
//    println("Pi is roughly " + 4.0 * count / n)
//    println("Pi is roughly " )
//    spark.stop()

  }

  def hellogll(sc:SparkContext)={
    val input=sc.textFile("/home/user/Templates/helloSpark")
    val lines=input.flatMap(line=>line.split(" "))
    val count=lines.map(word=>(word,1)).reduceByKey{case (x,y)=>x+y}
    //收集消息 从其他服务器上
    count.collect()
    val output=count.saveAsTextFile("/home/user/Templates/hellosparkRes")
  }

  def mapTest(sc:SparkContext) = {
    //读取 现券市场 的成交数据
    val file = sc.textFile("/home/user/Templates/ibo0001.txt",3)

    val mapResult = file.map(x =>{//map的特点是一个输入对应一条输出，没有返回值，对应的返回值会是(NULL)
    val info = x.split(",")
      (info(0),info(1),info(2),info(3),info(4),info(5),info(6),info(7))//转换成了元组
//    info.foreach((i:String)=>i)
    })
    //take是一个action，作用是取出前n条数据发送到driver，一般用于开发测试
    mapResult.take(10).foreach(println)
    println(" my test ====================================================================================")


    //map和mapPartition的区别：map是一条记录一条记录的转换，mapPartition是
    //一个partition（分区）转换一次
    val mapPartitionResult = file.mapPartitions(x => {//一个分区对应一个分区
    var info = new Array[String](3)
      for(line <- x) yield{//yield：作用：有返回值，所有的记录返回之后是一个集合
        info = line.split(",")
        (info(0),info(1),info(2),info(3),info(4),info(5),info(6),info(7))
      }
    })
    mapPartitionResult.take(10).foreach(println)
    //for(x<-mapPartitionResult.iterator())println(x)


    // 把一行转为多行记录，使用flatMap展平,把一条new_tweet记录转成两条login记录
    val flatMapTest = file.flatMap(x=>{
      val info = x.split(",")
      info(1) match {
        case "new_tweet"=> for (i <- 1 to 2) yield s"${info(0)} login ${info(2)}"
        case _ => Array(x)
      }
    })
    //flatMapTest.take(10).foreach(println)
    flatMapTest.take(14).foreach(println)
    println("file.count===================================================================================="+file.count())
    println("flatMapTest.coun"+flatMapTest.count())
  }
  def sparkPi(sc:SparkContext) = {

  }

}
