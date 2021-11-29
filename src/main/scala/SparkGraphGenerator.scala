import org.apache.spark.sql.SparkSession

object SparkGraphGenerator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkGraphGenerators").getOrCreate()

    val n = spark.sparkContext.broadcast(if (args.length > 0) args(0).toInt else 1000)
    val e = spark.sparkContext.broadcast(((2.0 / 3.0) * n.value * math.log(n.value) + (0.38481 * n.value)).toInt)

    val distData = spark.sparkContext.range(0, e.value)
    val pairs = distData.map(x => (new RMat().generateEdge(n.value)._1, 1))
    val degrees = pairs.reduceByKey(_ + _)
    val edges = degrees.flatMap(pair => createDirectedEdges(pair._1, pair._2))

    val path = System.getProperty("user.dir")
    val timestamp = System.currentTimeMillis()
    edges.saveAsTextFile(s"$path/directed-graph-$timestamp")
  }

  def createDirectedEdges(source: Long, degree: Int): Array[(Long, Long)] = {
    val edges = new Array[(Long, Long)](degree)
    var i: Int = 0
    var target: Long = source - 1
    while (i < degree && target >= 0) {
      edges(i) = (source, target)
      i += 1
      target -= 1
    }
    target = source + 1
    while (i < degree) {
      edges(i) = (source, target)
      i += 1
      target += 1
    }
    return edges
  }
}
