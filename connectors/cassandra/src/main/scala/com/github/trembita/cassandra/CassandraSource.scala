package com.github.trembita.cassandra

import cats.effect.Sync
import cats.implicits._
import com.datastax.driver.core.{Row, Session, Statement}
import com.github.trembita.internal.StrictSource
import com.github.trembita._

import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag

object CassandraSource {
  def rows(session: Session,
           statement: Statement): DataPipeline[Row, Execution.Sequential] =
    DataPipeline.from(session.execute(statement).iterator().asScala.toIterable)

  def rowsF[F[_], Ex <: Execution](session: Session, statement: Statement)(
    implicit F: Sync[F]
  ): DataPipelineT[F, Row, Ex] =
    new StrictSource[F, Row, Ex](F.delay {
      session.execute(statement).iterator().asScala
    }, F)

  def apply[A: ClassTag](session: Session, statement: Statement)(
    extractor: Row => A
  ): DataPipeline[A, Execution.Sequential] =
    rows(session, statement).map(extractor)

  def applyF[F[_], A: ClassTag, Ex <: Execution](session: Session, statement: Statement)(
    extractor: Row => A
  )(implicit F: Sync[F]): DataPipelineT[F, A, Ex] =
    rowsF[F, Ex](session, statement).map(extractor)
}
