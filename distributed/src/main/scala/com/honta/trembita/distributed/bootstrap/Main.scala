package com.honta.trembita.distributed.bootstrap


import com.honta.trembita.distributed._
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn


object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val clusterConfig = DistributedList.defaultConfig
    implicit val cluster = DistributedList.launchCluster(clusterConfig)

    logger.info("Cluster launched")

    val source: DistributedList[Int] = DistributedList(2)(() => (1 to 1000).iterator)
    val mapped: DistributedList[Int] = source.map(i => i * i)

    logger.info("Waiting workers registration...")
    Thread.sleep(5000)
    tryToCalculcate(mapped)
  }

  private def tryToCalculcate(list: DistributedList[Int]): Unit = try {
    val result: Iterable[Int] = list.force
    logger.info(s"Result: ${result.mkString(", ")}")
    StdIn.readLine("Repeat? (press Q for exit)") match {
      case "Q" => sys.exit(1)
      case _   => tryToCalculcate(list)
    }
  } catch {
    case e: Exception =>
      logger.error("Failed to get result, pause...", e)
      Thread.sleep(2000)
      tryToCalculcate(list)
  }
}
