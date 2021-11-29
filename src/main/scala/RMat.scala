import scala.annotation.tailrec
import scala.util.Random

class RMat {
  private val (alpha, beta, gamma, delta) = (0.75, 0.05, 0.19, 0.01)
  private var (offset1, offset2, offset3, offset4) = (0.0, 0.0, 0.0, 0.0)

  def generateEdge(n: Long): (Long, Long) = {
    val (a, b, c, d) = (alpha, beta, gamma, delta)
    val m = 0.25
    val depth = math.ceil(math.log(n) / math.log(2))
    offset1 = (m - a) / depth
    offset2 = (m - b) / depth
    offset3 = (m - c) / depth
    offset4 = (m - d) / depth
    return this.chooseEdge(1, 1, n, n, a, b, c, d)
  }

  @tailrec
  private def chooseEdge(x1: Long, y1: Long, xn: Long, yn: Long, a: Double, b: Double, c: Double, d: Double): (Long, Long) = {
    val r = Random.nextDouble()
    if (xn - x1 == 0 && yn - y1 == 0)
      return (x1, y1)
    if (xn - x1 == 0 && yn - y1 == 1)
      return if (r < 0.5) (x1, y1) else (x1, yn)
    if (xn - x1 == 1 && yn - y1 == 0)
      return if (r < 0.5) (x1, y1) else (xn, yn)

    val (ab, abc) = (a + b, a + b + c)
    val (newA, newB, newC, newD) = (math.abs(a + offset1), math.abs(b + offset2), math.abs(c + offset3), math.abs(d + offset4))

    if (r < a)
      return chooseEdge(x1, y1, math.floor((x1 + xn) / 2).toLong, math.floor((y1 + yn) / 2).toLong, newA, newB, newC, newD)
    else if (r >= a && r < ab)
      return chooseEdge(math.ceil((x1 + xn) / 2).toLong, y1, xn, math.floor((y1 + yn) / 2).toLong, newA, newB, newC, newD)
    else if (r >= ab && r < abc)
      return chooseEdge(x1, math.ceil((y1 + yn) / 2).toLong, math.floor((x1 + xn) / 2).toLong, yn, newA, newB, newC, newD)
    else
      return chooseEdge(math.ceil((x1 + xn) / 2).toLong, math.ceil((y1 + yn) / 2).toLong, xn, yn, newA, newB, newC, newD)
  }
}