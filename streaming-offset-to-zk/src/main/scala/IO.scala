import java.io.File

import scala.collection.generic.GenericCompanion
import scala.collection.{immutable, mutable}
import scala.io.{BufferedSource, Source}

/**
 * scala单词统计
 */
object IO {
  def main(args: Array[String]): Unit = {
    
  }

  def collect_first(): Unit ={
    val fun: PartialFunction[Any, Int] = {
      case x: Int => x + 1
    }
    val a: Array[Any] = Array('a',1,3, "A")
    val b: Option[Int] = a.collectFirst(fun)
    println(b) // Some(2)

    b match {
      case Some(a) => println(a)
      case None =>
    }

  }

  def collect_test(): Unit = {
    val fun: PartialFunction[String, Int] = {
      case "java" => 1
      case "scala" => 2
      case "python" => 3
      case _ => 0
    }
    val a: Array[String] = Array("java", "scala", "python", "php")
    val b: Array[Int] = a.collect(fun)
    println(b.toList) // 1,2,3,0
  }

  def add_then(): Unit = {
    val a = List(1, 2, 3, 4, 5, 6, 6, 7, 3)

    val b: Int = a.aggregate(5)(seqno, combine) // 不分区   中间参数为初始值
    println("b = " + b)
    /*
     * seq_exp = 5 + 1
     * seq_exp = 6 + 2
     * seq_exp = 8 + 3
     * seq_exp = 11 + 4
     * b = 15
     */

    val c: Int = a.par.aggregate(5)(seqno, combine) // 分区
    println("c = " + c)
    /*
     * seq_exp = 5 + 3
     * seq_exp = 5 + 2
     * seq_exp = 5 + 4
     * seq_exp = 5 + 1
     * com_exp = 6 + 7
     * com_exp = 8 + 9
     * com_exp = 13 +17
     * c = 30
     */
  }

  /**
   * 分区内计算
   *
   * @param m
   * @param n
   * @return
   */
  def seqno(m: Int, n: Int): Int = {
    val s = "seq_exp = %d + %d"
    println(s.format(m, n))
    m + n
  }

  /**
   * 分区计算后规约合并结果集
   *
   * @param m
   * @param n
   * @return
   */
  def combine(m: Int, n: Int): Int = {
    val s = "com_exp = %d + %d"
    println(s.format(m, n))
    m + n
  }


  def listFunction(): Unit = {
    val list: Range.Inclusive = 1 to 10
    println("==================partition=====================")
    val tuple: (immutable.IndexedSeq[Int], immutable.IndexedSeq[Int]) = list.partition(_ < 6)
    println(tuple._1)
    println(tuple._2)
    println("==================groupBy=====================")
    val map: Map[Int, immutable.IndexedSeq[Int]] = list.groupBy(_ % 5)
    for (key <- map) {
      println(key._1 + "=====>" + key._2)
    }
    println("==================grouped=====================")
    val iter: Iterator[immutable.IndexedSeq[Int]] = list.grouped(3)
    println(iter.toList)
    println("==================grouped=====================")
    val itar: Iterator[immutable.IndexedSeq[Int]] = list.sliding(3)
    println(itar.toList)
    val itar1: Iterator[immutable.IndexedSeq[Int]] = list.sliding(3, 2)
    println(itar1.toList)
  }

  def wordCount(): Unit = {
    val source: BufferedSource = Source.fromFile("D:\\DATA\\data.txt", "UTF-8")
    val iter: Iterator[String] = source.getLines() //返回文件中所有行数据，获得一个迭代器
    val list: List[String] = iter.toList
    val tuples: List[(String, Int)] = list.flatMap(_.split(" ")).map((_, 1)) //数据分割压平，标记1
    val result: Map[String, Int] = tuples.groupBy(_._1) //数据以key聚合，第二位为Array(（key,1）,...),再次根据第二个元素统计单词个数
      .map(
        (iter: (String, List[(String, Int)])) =>
          (iter._1, iter._2.map(_._2).sum)
      )
    //    val result: Map[String, Int] = source.getLines().toList.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map((iter: (String, List[(String, Int)])) => (iter._1, iter._2.map(_._2).sum))
    println(result)
    source.close()
  }
}
