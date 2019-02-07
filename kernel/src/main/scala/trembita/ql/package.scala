package trembita

import cats._

import language.{higherKinds, implicitConversions}
import language.experimental.macros
import shapeless._
import ql.AggRes._
import ql.AggDecl._
import ql.QueryBuilder._
import ql.GroupingCriteria._
import shapeless.syntax.SingletonOps

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

package object ql extends orderingInstances with aggregationInstances with monoidInstances with spire.std.AnyInstances with AggFunc.types {

  implicit class TaggingSyntax[A](private val self: A) extends AnyVal {
    def tagAs[T]: A :@ T = self.:@[T]
    def :@[T]: A :@ T    = new :@[A, T](self)
  }

  class aggDsl[A, U, AggT <: AggFunc.Type](val `f`: A => U) extends AnyVal {
    @inline def as(s: SingletonOps): A => TaggedAgg[U, s.T, AggT] = a => TaggedAgg(:@(`f`(a)))
  }
  class tagDsl[A, U](val `f`: A => U) extends AnyVal {
    @inline def as(s: SingletonOps): A => :@[U, s.T]                      = a => :@(`f`(a))
    @inline def agg[AggT <: AggFunc.Type](aggT: AggT): aggDsl[A, U, AggT] = new aggDsl[A, U, AggT](`f`)
  }
  class exprDsl[A](val `dummy`: Boolean = false) extends AnyVal {
    @inline def apply[U](f: A => U): tagDsl[A, U] = new tagDsl[A, U](f)
  }
  class havingDsl[A, T](val `f`: A => Boolean) extends AnyVal
  object havingDsl {
    trait Converter[A, T] {
      type AggR <: AggRes

      def apply(dsl: havingDsl[A, T]): AggR => Boolean
    }
    object Converter {
      @implicitNotFound("""
      Cannot perform having operation on aggregation result ${AggR0} using predicate for ${A} :@ ${T}
      Probably you using wrong aggregation tag for type ${A}.
      Please inspect what aggregation result you'd tagged with ${T}""")
      type Aux[A, T, AggR0 <: AggRes] = Converter[A, T] { type AggR = AggR0 }

      implicit def fromGet[A, T, AggR0 <: AggRes](implicit gget: AggRes.Get.Aux[AggR0, T, A]): Converter.Aux[A, T, AggR0] =
        new Converter[A, T] {
          type AggR = AggR0

          def apply(dsl: havingDsl[A, T]): AggR0 => Boolean = aggR => dsl.`f`(gget(aggR))
        }
    }
  }

  @inline def expr[A]: exprDsl[A]                                         = new exprDsl[A]()
  @inline def col[A]: tagDsl[A, A]                                        = new tagDsl[A, A](identity)
  @inline def agg[A](s: SingletonOps)(f: A => Boolean): havingDsl[A, s.T] = new havingDsl[A, s.T](f)

  implicit class GroupingCriteriaOps[G <: GroupingCriteria](private val self: G) extends AnyVal {
    def &::[GH <: :@[_, _]](head: GH): GH &:: G =
      GroupingCriteria.&::(head, self)

    def apply(n: Nat)(implicit at: GroupingCriteria.At[G, n.N]): at.Out =
      at(self)
  }

  implicit def tuple2GroupingCriteria[T, Out0 <: GroupingCriteria](t: T)(
      implicit ev: FromTuple.Aux[T, Out0]
  ): ev.Out = ev(t)

  implicit class AggregationNameOps[A <: AggDecl](val self: A) {
    def %::[GH <: TaggedAgg[_, _, _]](head: GH): GH %:: A =
      AggDecl.%::(head, self)
  }

  implicit def tuple2AggDecl[T, Out0 <: AggDecl](t: T)(
      implicit ev: FromTuple.Aux[T, Out0]
  ): ev.Out = ev(t)

  implicit class AggResOps[A <: AggRes](val self: A) {
    def *::[H <: :@[_, _]](head: H): H *:: A = AggRes.*::(head, self)

    def apply[U](u: U)(implicit get: AggRes.Get[A, U]): get.Out = get(self)

    def get[U](u: U)(implicit gget: AggRes.Get[A, U]): gget.Out = gget(self)
  }

  implicit class AsOps[F[_], Ex <: Environment, A, G <: GroupingCriteria, T](
      private val self: DataPipelineT[F, QueryResult[A, G, T], Ex]
  ) extends AnyVal {
    def as[R: ClassTag](implicit ev: ToCaseClass.Aux[A, G, T, R], F: Monad[F]): DataPipelineT[F, R, Ex] =
      self.mapImpl(_.as[R])
  }

  implicit def startTrembitaQl[F[_], A, E <: Environment](self: DataPipelineT[F, A, E]): Empty[F, A, E] =
    new Empty[F, A, E](self)

  implicit class QueryResultToCaseClass[A, K <: GroupingCriteria, T](
      private val self: QueryResult[A, K, T]
  ) extends AnyVal {
    def as[R](implicit ev: ToCaseClass.Aux[A, K, T, R]): R = ev(self)
  }
}
