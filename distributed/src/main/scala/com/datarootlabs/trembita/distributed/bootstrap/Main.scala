package com.datarootlabs.trembita.distributed.bootstrap

import scala.io.StdIn
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._
import com.datarootlabs.trembita.distributed._
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext.Implicits.global


object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val clusterConfig = DistributedDataPipeline.defaultConfig
    implicit val cluster = DistributedDataPipeline.launchCluster(clusterConfig)

    logger.info("Cluster launched")

    val source: DistributedDataPipeline[Int] = DistributedDataPipeline(2)(() => (1 to 1000).iterator)
    val mapped: DistributedDataPipeline[Int] = source.map(i => i * i)

    logger.info("Waiting workers registration...")
    Thread.sleep(5000)
    tryToCalculcate(mapped)
  }

  private def tryToCalculcate(list: DistributedDataPipeline[Int]): Unit = try {
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
