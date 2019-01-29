package trembita

object Test {
  def main(args: Array[String]): Unit = {
    val lines   = Vector("aa, b, cc - dd", "aex hello, world!", "aa hello, world!")
    val result1 = WordsCount.vector(lines).sortBy(_._1)
    val result2 = WordsCount.imperative(lines).sortBy(_._1)
    val result3 = WordsCount.pipelineNaive(lines).sortBy(_._1)
    val result4 = WordsCount.pipelinesAdvanced(lines).sortBy(_._1)
    val result5 = WordsCount.pipelinesAdvanced(lines, parallelism = 2).sortBy(_._1)

    val expected = lines.flatMap(_.split("\\W+")).groupBy(identity).mapValues(_.size).toVector.sortBy(_._1)

    assertEquals(result1, expected, "vector")
    assertEquals(result2, expected, "imperative")
    assertEquals(result3, expected, "pipeline naive")
    assertEquals(result4, expected, "pipeline advanced")
    assertEquals(result5, expected, "pipeline advanced with parallelism")
  }

  private def assertEquals(a: Any, b: Any, msg: String = ""): Unit =
    if (a != b) {
      println(s"""
           |Error $msg!
           |  expected: $b
           |  actual:   $a
         """.stripMargin)
    }
}
