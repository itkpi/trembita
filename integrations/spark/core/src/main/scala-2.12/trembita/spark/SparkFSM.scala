package trembita.spark

import cats.Id
import trembita.fsm.FSM.Func
import trembita.fsm._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.GroupState

import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

@implicitNotFound(
  """
    Unable to run Finite state machine in context ${F} on Spark.
    Probably Spark doesn't support an efficient implementation with ${F} by default
  """
)
trait SparkFSM[F[_]] {
  def byKey[A: ClassTag: Encoder, K: ClassTag: Encoder, N, D, B: TypeTag: Encoder](source: RDD[A])(
      getKey: A => K,
      initial: InitialState[N, D, F]
  )(fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]): RDD[B]
}

object SparkFSM {
  implicit def idFSM(implicit spark: SparkSession): SparkFSM[Id] =
    new SparkIDFsm
}

class SparkIDFsm(implicit val spark: SparkSession) extends SparkFSM[Id] {
  import spark.implicits._

  def byKey[A: ClassTag: Encoder, K: ClassTag: Encoder, N, D, B: TypeTag: Encoder](rdd: RDD[A])(
      getKey: A => K,
      initial: InitialState[N, D, Id]
  )(fsmF: FSM.Empty[Id, N, D, A, B] => Func[Id, N, D, A, B]): RDD[B] = {

    implicit val stateEncoder: Encoder[FSM.State[N, D, Id]] =
      Encoders.kryo(classOf[FSM.State[N, D, Id]])

    val fsm            = fsmF(new FSM.Empty[Id, N, D, A, B])
    val ds: Dataset[A] = rdd.toDS()
    val mappedDS: Dataset[Vector[B]] = ds
      .groupByKey(getKey)
      .mapGroupsWithState[FSM.State[N, D, Id], Vector[B]] { (k: K, as: Iterator[A], groupState: GroupState[FSM.State[N, D, Id]]) =>
        val currState: FSM.State[N, D, Id] = groupState.getOption match {
          case Some(state) => state
          case None =>
            initial match {
              case InitialState.Pure(state) => state
              case InitialState
                    .FromFirstElement(f: (A => FSM.State[N, D, Id]) @unchecked) =>
                val a = as.next()
                f(a)
            }
        }

        val (newState, newValues) =
          as.foldLeft((currState, Vector.empty[B])) {
            case ((accState, bs), a) =>
              val (stateNext, newBs) = fsm(accState)(a)
              (stateNext, bs ++ newBs)
          }
        groupState.update(newState)
        newValues
      }

    mappedDS.flatMap(x => x).rdd
  }
}
