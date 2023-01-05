import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, FileWriter}
import scala.util.Random
import scala.util.Random.shuffle
import scala.util.control.Breaks.break

object Prob2Deg2Shuffled {
  val t: Double = 2.01
  val c: Double = (2 * (t - 1)) / (1 + t)
  val undirected = false
  var numEdges: Int = 0

  def main(args: Array[String]): Unit = {
    val scale = if (args.length > 0) args(0).toInt else 23
    val path = if (args.length > 1) args(1) else System.currentTimeMillis().toString
    val numVertices = math.pow(2, scale).toInt
    numEdges = numEdges(numVertices, t)

    val conf = new SparkConf().setAppName("Prob2Deg2Shuffled")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println(s"Creating a graph of 2^$scale ($numVertices) vertices...")

    val startTime = System.currentTimeMillis()

    sc.parallelize(generateArrayPowerLaw(numVertices).zipWithIndex)
      .flatMap { case (degree, u) => createDirectedEdges(u, degree) }
      .map { case (u, v) => s"$u\t$v" }
      .saveAsTextFile(path)

    val timeSpent = System.currentTimeMillis() - startTime

    val log =
      s"""Prob2Deg2Shuffled
         |Scale $scale
         |Generation completed in ${timeSpent / 1000d} seconds""".stripMargin

    println(log)
    val fileWriter = new FileWriter(new File(path, s"scale$scale-${timeSpent / 1000d}s.txt"))
    fileWriter.write(log)
    fileWriter.close()

    sc.stop()
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
    edges
  }

  def generateProbabilities(n: Int, limit: Int): Array[Double] = {
    var probabilitiesSum: Double = 0
    val probabilities = new Array[Double](limit)
    probabilities(0) = 0  // leave it just to make it explicit

    for (i <- 1 until n) {
      val p: Double = c * math.pow(i, -t)
      if (i < limit) probabilities(i) = p
      probabilitiesSum += p
    }

    // normalize
    var cumulativeProb: Double = 0
    for (i <- 1 until limit) {
      cumulativeProb += probabilities(i) / probabilitiesSum
      probabilities(i) = cumulativeProb
    }
    probabilities
  }

  def generateDegrees(n: Int, limit: Int, cumProbArr: Array[Double]): Array[Int] = {
    val frequencies: Array[Int] = new Array[Int](limit)
    val x0 = 0
    val x1 = cumProbArr(limit - 1)
    val random = new Random()
    var r: Double = 0.0
    var i, j: Int = 0
    var edgesT = 0

//    for (k <- 0 until limit)  // Array are already initialized with 0's in Scala
//      frec(k) = 0                  // so setting them again is not necessary

    while (i < n) {
      r = math.pow((math.pow(x1, t + 1) - math.pow(x0, t + 1)) * random.nextDouble() + math.pow(x0, t + 1), 1 / (t + 1))
      j = 1
      while (j < limit) {
        if (r > cumProbArr(j - 1) && r < cumProbArr(j)) {
          frequencies(j) += 1
          edgesT += j
          j = limit
        }
        j += 1
      }
      if (undirected) {                      // undirected graph
        if (i == n && edgesT < numUndirectedEdges)
          i -= 1
      } else {                                   // directed graph
        if (edgesT >= numEdges)
          i = n + 1
      }
      i += 1
    }
    frequencies
  }

  def generateArrayPowerLaw(n: Int): Seq[Int] = {
    val degrees: Array[Int] = new Array[Int](n)
    val limit: Int = maxDegree(n, t)
    val probabilities = generateProbabilities(n, limit + 1)
    val frequencies = generateDegrees(n, limit + 1, probabilities)
    var h: Int = 0
    for (k <- limit to 1 by -1) {
      if (frequencies(k) > 0) {
        for (_ <- 0 until frequencies(k)) {
          degrees(h) = k
          h += 1
          if (h >= n)
            break
        }
      }
      if (h >= n)
        break
    }
    shuffle(degrees.toSeq)
  }

  def maxDegree(n: Int, t: Double): Int = (constant(t) * math.pow(n, exponent(t))).toInt

  def constant(t: Double): Double = 5.5 * math.pow(math.E, -math.pow(math.log(t) - 0.73, 2) / 0.04374) + 3.0321

  def exponent(t: Double): Double = math.pow(math.E, (-t + 1.54) * 5.05) + 0.457

  def numEdges(n: Int, t: Double): Int = ((n * (t - 1) * (2 * math.pow(n, 2 - t) - t)) / ((1 + t) * (2 - t))).toInt

  def numUndirectedEdges: Int = 2 * numEdges
}
