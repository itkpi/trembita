package com.github.trembita.spark.streaming

import cats.Monad
import com.github.trembita.DataPipelineT
import com.github.trembita.outputs.internal.OutputT
import scala.reflect.ClassTag

class StartOutput[F[_], A] private[trembita] (sync: (() => Unit) => F[Unit]) extends OutputT[F, A, SparkStreaming] {
  final type Out[G[_], b] = G[Unit]

  def apply(pipeline: DataPipelineT[F, A, SparkStreaming])(
      implicit F: Monad[F],
      E: SparkStreaming,
      run: RunOnSparkStreaming[F],
      A: ClassTag[A]
  ): F[Unit] = F.flatMap(pipeline.evalRepr) { stream =>
    sync { () =>
      stream.context.start()
    }
  }
}
