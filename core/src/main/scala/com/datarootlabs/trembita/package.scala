package com.datarootlabs


import cats._
import cats.data._
import cats.implicits._
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._


package object trembita {
  type PairPipeline[K, V] = DataPipeline[(K, V)]
  type ParPairPipeline[K, V] = ParDataPipeline[(K, V)]

  implicit class PairPipelineOps[K, V](val self: PairPipeline[K, V]) extends AnyVal {
    def mapValues[W](f: V => W): PairPipeline[K, W] = self.map { case (k, v) => (k, f(v)) }
    def reduceByKey(f: (V, V) => V): PairPipeline[K, V] =
      self.groupBy(_._1).mapValues { vs =>
        vs.foldLeft(Option.empty[V]) {
          case (None, (_, v)) => Some(v)
          case (acc, (_, v))  => acc.map(f(_, v))
        }.get
      }
    def toMap: MapPipeline[K, V] = new BaseMapPipeline[K, V](self)
  }

  implicit object DataPipelineMonad extends Monad[DataPipeline] {
    override def pure[A](x: A): DataPipeline[A] = DataPipeline(x)
    override def flatMap[A, B](fa: DataPipeline[A])(f: A ⇒ DataPipeline[B]): DataPipeline[B] = fa.flatMap(f(_).force)
    override def tailRecM[A, B](a: A)(f: A ⇒ DataPipeline[Either[A, B]]): DataPipeline[B] = f(a).flatMap {
      case Left(xa) ⇒ tailRecM(xa)(f).force
      case Right(b) ⇒ Some(b)
    }
  }
}
