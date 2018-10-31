package scala

import scala.concurrent.Future
//import com.twitter.util.{Future,Promise}

/**
  * Created by yaohou on 0:46 2017/12/20.
  * description: 
  */
object Test {
  def main(args: Array[String]): Unit = {
    //println(1 + 1)
    //不变量（val）
    val two = 1 + 1

    //变量
    var name = "houyao"
    name = "gll"

    //函数
    def addOne(m: Int): Int = m + 1

    //addOne: (m: Int)Int
    val three = addOne(2)

    //three: Int = 3
    //如果函数不带参数，你可以不写括号
    def defThree() = 1 + 2

    defThree() //defThree() & defThree
    //匿名函数
    //(x: Int) => x + 1   Exception
    println(2)
    // print(1) = print((1))
    //传递匿名函数，或将其保存成不变量
    val inner = (x: Int) => x + 1
    inner(1)
    //如果你的函数有很多表达式，可以使用 {} 来格式化代码，使之易读。  see class out
    //def timeTwo(j:Int):Int = {print("hello scala") j*2}
    //def timesTwo(i: Int): Int = {  println("hello world")  }

    //部分应用（Partial application） ***  i don't understand
    //你可以使用下划线“”部分应用一个函数，结果将得到另一个函数。Scala 使用下划线表示不同上下文中的不同事物，你通常可以把它看作是一个没有命名的神奇通配符。在`{ + 2 }`的上下文中，它代表一个匿名参数。你可以这样使用它：
    def adder(m: Int, n: Int) = m + n

    // export：adder: (m: Int,n: Int)Int
    val add2 = adder(2, _: Int)
    // export： add2: (Int) => Int = <function1>
    add2(3) //export：res50: Int = 5

    //柯里化函数
    //有时会有这样的需求：允许别人一会在你的函数上应用一些参数，然后又应用另外的一些参数。      例如一个乘法函数，在一个场景需要选择乘数，而另一个场景需要选择被乘数。
    //def multiply(m: Int)(n: Int): Int = m * nmultiply: (m: Int)(n: Int)Int
    def multiply(m: Int)(n: Int): Int = m * n //multiply:(m:Int)(n:Int) Int
    var valuwMu = multiply(2)(3)
    val timesTwo = multiply(2) _ // export:   timesTwo: (Int) => Int = <function1>
    timesTwo(3) // export : res1: Int = 6

    // i don't understand
    (adder _) //.curriedres1: (Int) => (Int) => Int = <function1>

    //可变长度参数
    //这是一个特殊的语法，可以向方法传入任意多个同类型的参数。例如要在多个字符串上执行 String 的 capitalize 函数，可以这样写：
    def capitalizeAll(args: String*) = {
      args.map { arg => arg.capitalize }
    }

    capitalizeAll("rarity", "applejack") //export: res2: Seq[String] = ArrayBuffer(Rarity, Applejack)

    // 下面 面的例子展示了如何在类中用 def 定义方法和用 val 定义字段值。方法就是可以访问类的状态的函数。
    val calc = new Calculator("gll") //calc//: Calculator = Calculator@e75a11scala>
    calc.add(1, 2)
    //res1: Int = 3
    //calc.brand//res2//: String = "HP"
    // 构造函数
    val constru = new Constructor("HP")
    println(constru.color)
    //表达式 class :Constructor
    // 上文的 Calculator 例子说明了 Scala 是如何面向表达式的。颜色的值就是绑定在一个if/else表达式上的。Scala 是高度面向表达式的：大多数东西都是表达式而非指令。

    //函数 vs 方法
    //函数和方法在很大程度上是可以互换的。由于函数和方法是如此的相似，你可能都不知道你调用的东西是一个函数还是一个方法。而当真正碰到的方法和函数之间的差异的时候，你可能会感到困惑。
    val c = new C
    c.acc // function
    c.minc //method

    //abstract
    //val s = new Shape//<console>:8: error: class Shape is abstract; cannot be instantiated       val s = new Shape               ^scala>
    val cir = new Circle(2) //c: Circle = Circle@65c0035b

    // 特质
    //tratis  特质是一些字段和行为的集合，可以扩展或混入（mixin）你的类中。
//    trait Car {
//      val brand: String
//    } trait Shiny {
//      val shineRefraction: Int
//    } class BMW extends Car {
//      val brand = "BMW"
//    }
    //通过 with 关键字，一个类可以扩展多个特质：
    //    class BMW extends Car with Shiny {
    //      val brand = "BMW"
    //      val shineRefraction = 12
    //    }

    /*
    choice  abstract or tratis
     */
    //优先使用特质。一个类扩展多个特质是很方便的，但却只能扩展一个抽象类。
    //如果你需要构造函数参数，使用抽象类。因为抽象类可以定义带参数的构造函数，而特质不行。例如，你不能说trait t(i: Int) {}，参数i是非法的。

    //类型
    //此前，我们定义了一个函数的参数为 Int，表示输入是一个数字类型。其实函数也可以是泛型的，来适用于所有类型。当这种情况发生时，你会看到用方括号语法引入的类型参数。下面的例子展示了一个使用泛型键和值的缓存。
    trait Cache[K, V] {
      def get(key: K): V

      def put(key: K, value: V)

      def delete(key: K)
    }

    //方法也可以引入类型参数。
    //def remove[K](key: K)
    //匹配类成员
    def calcType(calc: Calculator) = calc match {
      case _ if calc.brands == "hp" && calc.model == "20B" => "financial"
      case _ if calc.brands == "hp" && calc.model == "48G" => "scientific"
      case _ if calc.brands == "hp" && calc.model == "30B" => "business"
      case _ => "unknown"
    }

    // 样本类  Case Classes
    //使用样本类可以方便得存储和匹配类的内容。你不用 new 关键字就可以创建它们。
    //case class Calculator(brand: String, model: String)
    //val hp20b = Calculator("hp", "20b")
    //样本类基于构造函数的参数，自动地实现了相等性和易读的 toString 方法。
    //样本类也可以像普通类那样拥有方法。
    val hp20b = Calculators("hp", "20B")
    val hp30b = Calculators("hp", "30B")

//    def calcTypes(calcs: Calculator) = calcs match {
//      case Calculators("hp", "20B") => "financial"
//      case Calculators("hp", "48G") => "scientific"
//      case Calculators("hp", "30B") => "business"
//      case Calculators(ourBrand, ourModel) => "Calculator: %s %s is of unknown type".format(ourBrand, ourModel)
//      //最后一句也可以这样写
//      case Calculators(_, _) => "Calculator of unknown type"
//      //或者我们完全可以不将匹配对象指定为 Calculator 类型
//      case _ => "Calculator of unknown type"
//      //或者我们也可以将匹配的值重新命名。
//      case c@Calculators(_, _) => "Calculator: %s of unknown type".format(c)

      //异常
      //Scala 中的异常可以在 try-catch-finally 语法中通过模式匹配使用。
      //    try {
      //    remoteCalculatorService.add(1, 2)
      //    } catch {
      //    case e: ServerIsDownException => log.error(e, "the remote calculator service is unavailable. should have kept your trusty HP.")
      //    } finally {
      //    remoteCalculatorService.close()
      //    }
      //        try 也是面向表达式的
      //        val result: Int = try {
      //          remoteCalculatorService.add(1, 2)
      //        } catch {
      //          case e: ServerIsDownException => {
      //            log.error(e, "the remote calculator service is unavailable. should have kept your trusty HP.")
      //            0
      //          }
      //        } finally {
      //          remoteCalculatorService.close()
      //        }


  }

  class C {
    var acc = 0

    def minc = {
      acc += 1
    }

    val finc = { () => acc += 1 }
  }

  // 继承 extends    参考 Effective Scala 指出如果子类与父类实际上没有区别，类型别名是优于继承的。A Tour of Scala 详细介绍了子类化。
  class ScientificCalculator(brand: String) extends Calculator(brand) {
    def log(m: Double, base: Double) = math.log(m) / math.log(base)
  }

  //重载  overload
  class EvenMoreScientificCalculator(brand: String) extends ScientificCalculator(brand) {
    def log(m: Int): Double = log(m, math.exp(1))
  }

  //抽象类
  abstract class Shape {
    def getArea(): Int // subclass should define this
  }

  //defined class Shapescala>
  class Circle(r: Int) extends Shape {
    def getArea(): Int = {
      r * r * 3
    }
  }

  //defined class Circlescala>


  { i: Int => println("hello world") }


}
