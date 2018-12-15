package com.github.trembita

import cats.{Id, Monad}
import com.github.trembita.internal.StrictSource
import scala.reflect.ClassTag

object DataPipeline {

  /**
    * Wraps given elements into a [[DataPipelineT]]
    *
    * @param xs - elements to wrap
    * @return - a [[StrictSource]]
    **/
  def apply[A: ClassTag](xs: A*): DataPipeline[A, Execution.Sequential] =
    new StrictSource[Id, A, Execution.Sequential](xs.toIterator, Monad[Id])

  /**
    * Wraps an [[Iterable]] passed by-name
    *
    * @param it - an iterable haven't been evaluated yet
    * @return - a [[StrictSource]]
    **/
  def from[A: ClassTag](
    it: => Iterable[A]
  ): DataPipeline[A, Execution.Sequential] =
    new StrictSource[Id, A, Execution.Sequential](it.toIterator, Monad[Id])
}
