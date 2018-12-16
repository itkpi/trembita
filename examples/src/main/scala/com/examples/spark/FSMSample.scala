package com.examples.spark

import cats.Id
import cats.effect.{ExitCode, IO, IOApp}
import com.github.trembita._
import com.github.trembita.experimental.spark._
import org.apache.spark._
import cats.syntax.all._
import com.examples.putStrLn
import com.github.trembita.fsm._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import com.github.trembita.collections._
import scala.concurrent.duration._

/**
  * To run this example, you need a spark-cluster.
  * Use docker-compose to deploy one
  *
  * @see resources/spark/cluster
  * */
object FSMSample extends IOApp {
  sealed trait DoorState extends Serializable
  case object Opened extends DoorState
  case object Closed extends DoorState

  implicit val doorStateEncoder: Encoder[DoorState] = Encoders.kryo[DoorState]
  implicit val stateEncoder: Encoder[Map[DoorState, Int]] =
    Encoders.kryo[Map[DoorState, Int]]

  def sparkSample(implicit spark: SparkSession): IO[Unit] = {
    import spark.implicits._

    val pipeline: DataPipelineT[Id, Int, Spark] =
      DataPipelineT.fromRepr[Id, Int, Spark](
        spark.sparkContext.parallelize(
          List.tabulate(5000)(i => scala.util.Random.nextInt() + i)
        )
      )

    val withDoorState =
      pipeline
        .fsmByKey[Int, DoorState, Map[DoorState, Int], Int](getKey = _ % 4)(
          initial = InitialState.pure(FSM.State(Opened, Map.empty))
        )(_.when(Opened) {
          case i if i % 2 == 0 =>
            _.goto(Closed)
              .modify(_.modify(Opened, default = 1)(_ + 1))
              .push(_.apply(Opened) + i)
          case i if i % 4 == 0 => _.stay push (i * 2)
        }.when(Closed) {
            case i if i % 3 == 0 =>
              _.goto(Opened)
                .modify(_.modify(Closed, default = 1)(_ + 1)) spam (_.apply(
                Closed
              ) to 10)
            case i if i % 2 == 0 =>
              _.stay.push(_.values.sum)
          }
          .whenUndefined { i =>
            {
              println(s"Producing nothing..! [#$i]")
              _.goto(Closed).change(Map.empty).dontPush
            }
          })
        .mapK(idToIO)
        .map(_ + 1)

    withDoorState
      .evalWith(AsyncTimeout(5.minutes))
      .flatTap(putStrLn)
      .void
  }
  def run(args: List[String]): IO[ExitCode] = {
    IO(
      SparkSession
        .builder()
        .master("spark://spark-master:7077")
        .appName("trembita-spark")
        .getOrCreate()
    ).bracket(use = { implicit spark: SparkSession =>
        sparkSample
      })(release = spark => IO { spark.stop() })
      .as(ExitCode.Success)
  }
}
