package com.github.trembita.spark

import cats.Monad
import com.github.trembita.DataPipelineT
import com.github.trembita.outputs.internal.OutputT
import scala.language.higherKinds
import scala.reflect.ClassTag

class ArrayOutput[F[_], A] private[trembita] () extends OutputT[F, A, Spark] {
  final type Out[G[_], β] = G[Array[β]]

  def apply(pipeline: DataPipelineT[F, A, Spark])(
      implicit F: Monad[F],
      E: Spark,
      run: RunOnSpark[F],
      A: ClassTag[A]
  ): F[Array[A]] = F.map(pipeline.evalRepr)(_.collect())
}
