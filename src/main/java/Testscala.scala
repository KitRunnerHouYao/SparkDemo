import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yaohou on 9:23 2018/7/13.
  * description:
  */
object Testscala {

  def main(args: Array[String]): Unit = {
    val world = "hello world!"
    println("test begin:"+world)

    val conf=new SparkConf().setAppName("wordcount").setMaster("local")
    //初始化SparkContext - sc.
    val sc=new SparkContext(conf)

    val rdd=sc.parallelize(List("this is a test","how are you","do you love me","can you tell me"))

    val words =rdd.flatMap(line => line.split(" "))
    val results=words.map(word => (word,1)).reduceByKey(_+_)
    results.foreach(println)
    println(args.foreach(x => x))




  }

}
