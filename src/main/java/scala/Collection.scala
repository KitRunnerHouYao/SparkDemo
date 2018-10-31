package scala

/**
  * Created by yaohou on 1:19 2017/12/22.
  * description: 
  */
object Collection {
  def main(args: Array[String]): Unit = {
    //List
    val listNum = List(1,2,3)
    // set :is do not repeat
    val setNum = Set()
    // Tuple :元组是在不使用类的前提下，将元素组合起来形成简单的逻辑集合。
    val httphost = ("localhost",8089)
      //与样本类不同，元组不能通过名称获取字段，而是使用位置下标来读取对象；而且这个下标基于 1，而不是基于 0。
    println(httphost._1)
    //元组可以很好得与模式匹配相结合。

    httphost match {
      case ("localhost", port) => "localhost"
      case (host, port) => "a,o"
    }

    //在创建两个元素的元组时，可以使用特殊语法：->
    var tuple = 1 -> 2

    //映射 Map
    //它可以持有基本数据类型。
    var mapnum = Map(1 -> 2)
    var mapnum2 = Map("foo" -> "bar")
    println(mapnum2.+("foo"))
    var mapnum3 = Map(1 -> "one", 2 -> "two")
    val ao = mapnum3.get(2)


    //选项 Option
    // Option 是一个表示有可能包含值的容器。
    //Option基本的接口是这样的：
    trait Option[T] {
      def isDefined: Boolean
      def get: T
      def getOrElse(t: T): T
    }
  }

}
