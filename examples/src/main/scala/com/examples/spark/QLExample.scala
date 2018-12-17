package com.examples.spark
import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.sql._
import com.github.trembita._
import cats.implicits._
import com.examples.putStrLn
import com.github.trembita.experimental.spark._
import com.github.trembita.ql._
import scala.concurrent.duration._

object QLExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    trait `divisible by 2`
    trait `divisible by 3`
    trait `reminder of 4`

    trait square
    trait count
    trait `^4`
    trait `some name`

    IO(SparkSession.builder().master("local[*]").appName("test").getOrCreate())
      .bracket(use = { implicit spark: SparkSession =>
        import spark.implicits._
        implicit val timeout: AsyncTimeout = AsyncTimeout(5.minutes)
        val numbers: DataPipelineT[IO, Long, Spark] =
          DataPipelineT
            .liftF[IO, Long, Sequential](IO { 1L to 20L })
            .to[Spark]

        val result = numbers
          .query(
            _.filter(_ > 5)
              .groupBy(
                num =>
                  (
                    (num % 2 == 0).as[`divisible by 2`],
                    (num % 3 == 0).as[`divisible by 3`],
                    (num % 4).as[`reminder of 4`]
                )
              )
              .aggregate(
                num =>
                  (
                    (num * num).toDouble.as[square].avg,
                    num.as[count].count,
                    (num * num * num * num).as[`^4`].sum,
                    num.toString.as[`some name`].sum
                )
              )
              .having(_.get[count] > 7)
          )

        result.eval.flatTap(putStrLn)
      })(
        release = spark =>
          IO {
            spark.close()
        }
      )
      .map(_ => ExitCode.Success)
  }
}
