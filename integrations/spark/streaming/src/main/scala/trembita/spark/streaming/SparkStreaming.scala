package trembita.spark.streaming

import cats.Monad
import trembita._
import trembita.spark.BaseSpark
import org.apache.spark.streaming.dstream.DStream

import scala.language.higherKinds
import scala.reflect.ClassTag

sealed trait SparkStreaming extends Environment with BaseSpark {
  final type Repr[X]   = DStream[X]
  final type Run[G[_]] = RunOnSparkStreaming[G]
  final type Result[X] = Unit

  val FlatMapResult: Monad[Result] = new Monad[Result] {
    override def pure[A](x: A): Unit                         = {}
    override def flatMap[A, B](fa: Unit)(f: A => Unit): Unit = {}
    override def tailRecM[A, B](a: A)(f: A => Unit): Unit    = {}
  }
  val FlatMapRepr: ApplicativeFlatMap[Repr] = new ApplicativeFlatMap[DStream] {
    def map[A, B: ClassTag](fa: DStream[A])(
        f: A => B
    ): DStream[B] = fa.map(f)
    def mapConcat[A, B: ClassTag](fa: DStream[A])(
        f: A => Iterable[B]
    ): DStream[B] = fa.flatMap(f)
  }
  val TraverseRepr: TraverseTag[Repr, Run] = new TraverseTag[Repr, Run] {
    def traverse[G[_], A, B: ClassTag](fa: DStream[A])(f: A => G[B])(
        implicit G: RunOnSparkStreaming[G]
    ): G[DStream[B]] = G.lift(G.traverse(fa)(f))
  }

  def toVector[A](repr: Repr[A]): Result[Vector[A]] = {}

  def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit] = repr.map(f)

  def groupBy[A, K: ClassTag](vs: Repr[A])(f: A => K): Repr[(K, Iterable[A])] = ???
  /*  vs.map(a => f(a) -> a).groupByKey()*/

  def collect[A, B: ClassTag](repr: Repr[A])(pf: PartialFunction[A, B]): Repr[B] =
    repr.flatMap(pf.lift(_))

  def distinctKeys[A: ClassTag, B: ClassTag](repr: Repr[(A, B)]): Repr[(A, B)] =
    repr.reduceByKey((b1, b2) => b1)

  def concat[A](xs: Repr[A], ys: Repr[A]): Repr[A] =
    xs union ys

  def zip[A, B: ClassTag](xs: Repr[A], ys: Repr[B]): Repr[(A, B)] = ???

  def memoize[A: ClassTag](xs: Repr[A]): Repr[A] = xs.persist()
}

object SparkStreaming {
  implicit val SparkStreaming: SparkStreaming = new SparkStreaming {}
}
