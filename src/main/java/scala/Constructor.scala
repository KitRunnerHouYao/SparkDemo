package scala

/**
  * Created by yaohou on 0:42 2017/12/21.
  * description: 
  */
class Constructor(brand: String) {
  /** * A constructor.   */
  val color: String = if (brand == "TI") {
    "blue"
  } else if (brand == "HP") {
    "black"
  } else {
    "white"
  }
  // An instance method.  def add(m: Int, n: Int): Int = m + n
  //val calc = new Calculator("HP")calc: Calculator = Calculator@1e64cc4dscala> calc.colorres0: String = blac
//  val constru = new Constructor("HP")
//  println(constru.color)
}
