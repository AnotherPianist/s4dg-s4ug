import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object S4DG_S4UG {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Invalid parameters. Usage: \"spark-submit [filename.jar] [-d/-u] [num of nodes] [-r] [randomness]\"")
      sys.exit(-1)
    }

    val n: Long = args(1).toLong
    val e: Long = ((2.0 / 3.0) * n * math.log(n) + (0.38481 * n)).toLong
    val r: Double = if (args.length > 2) args(3).toDouble else 1.0
    val directed: Boolean = if (args.contains("-u")) false else true
    println(s"Creating ${if (directed) "" else "un"}directed graph with $n nodes and $e edges")

    val conf = new SparkConf().setAppName("S4DG - S4UG")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val completeNodesSlice = sc.range(0, n).map(x => (x, 1))
    val fillingSlice = sc.range(0, e - n).map(_ => (generateEdge(n)._1, 1))
    val pairs = completeNodesSlice ++ fillingSlice
    val degrees = pairs.reduceByKey(_ + _)
    val edges = degrees.flatMap(pair => if (directed) createDirectedEdges(pair._1, pair._2, r) else createUndirectedEdges(pair._1, pair._2, n, r))

    val path = System.getProperty("user.dir")
    val timestamp = System.currentTimeMillis()
    edges.saveAsTextFile(s"$path/${if (directed) "" else "un"}directed-graph-$timestamp")
  }

  def createDirectedEdges(source: Long, degree: Int, randomness: Double = 1.0): Array[(Long, Long)] = {
    val edges = new Array[(Long, Long)](degree)
    var i: Int = 0
    var target: Long = source - 1
    while (i < degree && target >= 0) {
      if (Random.nextDouble() < randomness) {
        edges(i) = (source, target)
        i += 1
      }
      target -= 1
    }
    target = source + 1
    while (i < degree) {
      if (Random.nextDouble() < randomness) {
        edges(i) = (source, target)
        i += 1
      }
      target += 1
    }
    edges
  }

  def createUndirectedEdges(source: Long, degree: Int, n: Long, randomness: Double = 1.0): Array[(Long, Long)] = {
    val edges = new Array[(Long, Long)](degree)
    var i: Int = 0
    var target: Long = source + 1
    while (i < degree && target < n) {
      if (Random.nextDouble() < randomness) {
        edges(i) = (source, target)
        i += 1
      }
      target += 1
    }
    target = 0
    while (i < degree) {
      if (Random.nextDouble() < randomness) {
        edges(i) = (source, target)
        i += 1
      }
      target += 1
    }
    edges
  }

  def generateEdge(n: Long): (Long, Long) = {
    var (a, b, c, d) = (0.75, 0.05, 0.19, 0.01)
    val m = 0.25
    val depth = math.ceil(math.log(n) / math.log(2))
    val (offset1, offset2, offset3, offset4) = ((m - a) / depth, (m - b) / depth, (m - c) / depth, (m - d) / depth)
    var (x1, y1, xn, yn) = (1.toLong, 1.toLong, n, n)

    while (xn - x1 > 1 || yn - y1 > 1) {
      val r = Random.nextDouble()
      if (r < a) {
        xn = math.floor((x1 + xn) / 2).toLong
        yn = math.floor((y1 + yn) / 2).toLong
      } else if (r < a + b) {
        x1 = math.ceil((x1 + xn) / 2).toLong
        yn = math.floor((y1 + yn) / 2).toLong
      } else if (r < a + b + c) {
        y1 = math.ceil((y1 + yn) / 2).toLong
        xn = math.floor((x1 + xn) / 2).toLong
      } else {
        x1 = math.ceil((x1 + xn) / 2).toLong
        y1 = math.ceil((y1 + yn) / 2).toLong
      }

      a = math.abs(a + offset1)
      b = math.abs(b + offset2)
      c = math.abs(c + offset3)
      d = math.abs(d + offset4)
    }

    val r = Random.nextDouble()
    if (xn - x1 == 0 && yn - y1 == 1)
      if (r < 0.5) (x1, y1) else (x1, yn)
    else if (xn - x1 == 1 && yn - y1 == 0)
      if (r < 0.5) (x1, y1) else (xn, yn)
    else
      (x1, y1)
  }
}
