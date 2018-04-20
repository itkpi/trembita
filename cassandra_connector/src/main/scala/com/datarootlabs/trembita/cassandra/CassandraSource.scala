package com.datarootlabs.trembita.cassandra


import cats.MonadError
import cats.implicits._
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.internal.StrictSource
import com.datastax.driver.core.{Row, Session, Statement}
import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.util.Try


object CassandraSource {
  def rows(session: Session, statement: Statement): DataPipeline[Row, Try, Finiteness.Finite, Execution.Sequential] =
    DataPipeline.from(session.execute(statement).iterator().asScala.toIterable)

  def rowsF[F[_], Ex <: Execution](session: Session, statement: Statement)(implicit me: MonadError[F, Throwable])
  : DataPipeline[Row, F, Finiteness.Finite, Ex] = new StrictSource[Row, F, Finiteness.Finite, Ex](
    session.execute(statement).iterator().asScala.pure[F]
  )

  def apply[A](session: Session, statement: Statement)(extractor: Row => A): DataPipeline[A, Try, Finiteness.Finite, Execution.Sequential] =
    rows(session, statement).map(extractor)

  def applyF[A, F[_], Ex <: Execution]
  (session: Session, statement: Statement)(extractor: Row => A)
  (implicit me: MonadError[F, Throwable])
  : DataPipeline[A, F, Finiteness.Finite, Ex] =
    rowsF[F, Ex](session, statement).map(extractor)
}
