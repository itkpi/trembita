//package com.examples.spark
//
//import cats.effect.{ExitCode, IO, IOApp}
//import org.apache.spark.sql._
//import com.github.trembita._
//import cats.implicits._
//import com.examples.putStrLn
//import com.github.trembita.experimental.spark._
//import com.github.trembita.ql._
//import scala.concurrent.duration._
//import shapeless.syntax.singleton._
//
//object QLExample extends IOApp {
//  def run(args: List[String]): IO[ExitCode] = {
//    IO(SparkSession.builder().master("local[*]").appName("test").getOrCreate())
//      .bracket(use = { implicit spark: SparkSession =>
//        import spark.implicits._
//        implicit val timeout: AsyncTimeout = AsyncTimeout(5.minutes)
//        val numbers: DataPipelineT[IO, Long, Spark] =
//          DataPipelineT
//            .liftF[IO, Long, Sequential](IO { 1L to 20L })
//            .to[Spark]
//
//        val result = numbers
//          .query(
//            _.where(_ > 5)
//              .groupBy(
//                expr[Long](_ % 2 == 0) as "divisible by 2",
//                expr[Long](_ % 3 == 0) as "divisible by 3",
//                expr[Long](_ % 4) as "reminder of 4"
//              )
//              .aggregate(
//                expr[Long](num => (num * num).toDouble) agg avg as "square",
//                col[Long] agg count as "count",
//                expr[Long](num => num * num * num * num) agg sum as "^4",
//                expr[Long](_.toString) agg sum as "some name"
//              )
//              .having(agg[Long]("count")(_ > 7))
//          )
//
//        result.eval.flatTap(putStrLn)
//      })(
//        release = spark =>
//          IO {
//            spark.close()
//        }
//      )
//      .map(_ => ExitCode.Success)
//  }
//}
