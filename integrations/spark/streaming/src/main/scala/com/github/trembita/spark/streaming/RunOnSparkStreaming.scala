package com.github.trembita.spark.streaming

import cats.Applicative
import com.github.trembita.spark.RunOnSpark
import org.apache.spark.streaming.dstream.DStream

import scala.language.higherKinds
import scala.reflect.ClassTag

class RunOnSparkStreaming[F[_]](runOnSpark: RunOnSpark[F])(implicit F: Applicative[F]) extends Serializable {
  def traverse[A, B: ClassTag](a: DStream[A])(f: A => F[B]): DStream[B] =
    a.transform(rdd => runOnSpark.traverse(rdd)(f))

  def lift[A](dstream: DStream[A]): F[DStream[A]] = F.pure(dstream)
}

object RunOnSparkStreaming {
  implicit def fromRunOnSpark[F[_]: Applicative](implicit ev: RunOnSpark[F]): RunOnSparkStreaming[F] =
    new RunOnSparkStreaming[F](ev)
}
