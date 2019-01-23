package trembita.ql

import trembita.ql.AggDecl.{%::, DNil}
import trembita.ql.GroupingCriteria.{&::, GNil}
import scala.annotation.implicitNotFound
import scala.language.implicitConversions

trait ExprMagnet[T] extends Serializable {
  type Out

  def apply(): Out
}

object ExprMagnet {
  @implicitNotFound("""
      Unable to make expression ${Out0} from ${T}.
      In most cases it means that you messed up with your query
    """)
  type Aux[T, Out0] = ExprMagnet[T] { type Out = Out0 }

  def apply[T](implicit ev: ExprMagnet[T]): Aux[T, ev.Out] = ev

  implicit def groupByFromSingleExpr[A, H <: :@[_, _]](f: A => H): ExprMagnet.Aux[A => H, A => H &:: GNil] =
    new ExprMagnet[A => H] {
      type Out = A => H &:: GNil

      def apply(): Out = a => f(a) &:: GNil
    }

  implicit def aggFromSinleExpr[A, T <: TaggedAgg[_, _, _]](f: A => T): ExprMagnet.Aux[A => T, A => T %:: DNil] =
    new ExprMagnet[A => T] {
      type Out = A => T %:: DNil

      def apply(): A => T %:: DNil = a => f(a) %:: DNil
    }
}
