package trembita

object OhMyGoodnessBench {
  def main(args: Array[String]): Unit = {
    val file = new WordsCount.OhMyGoodnessFile()
    file.init()
    val result0 = measure("pipelineNaive", times = 10) {
      WordsCount.pipelineNaive(file.ohNooo)
    }
    val result1 = measure("iterator", times = 10) {
      WordsCount.imperative(file.ohNooo)
    }
    println(result0)
    println(result1)
  }

  def measure[U](name: String, times: Int)(thunk: => U): String = {
    val warmup = {
      println(s"$name: warmup")
      for (i <- 1 to times) {
        println(s"\t$name warmup #$i: started")
        val start = System.currentTimeMillis()
        thunk
        println(s"\t$name warmup #$i: elapsed (${System.currentTimeMillis() - start}) ms")
      }
    }
    println(s"$name: test")
    var results = List.empty[Long]
    for (i <- 1 to times) {
      println(s"\t$name #$i: started")
      val start = System.currentTimeMillis()
      thunk
      val elapsed = System.currentTimeMillis() - start
      results :+= elapsed
      println(s"\t$name #$i: elapsed ($elapsed) ms")
    }
    s"$name: count=$times, avgt=${results.sum / times}"
  }
}
