package com.github.trembita.cassandra.phantom

import cats.MonadError
import cats.effect.Sync
import com.github.trembita._
import com.datastax.driver.core.{ProtocolVersion, Session}
import com.github.trembita.cassandra.CassandraSource
import com.outworkers.phantom.{Table, Row => PhantomRow}
import com.outworkers.phantom.builder.query.SelectQuery
import com.outworkers.phantom.connectors.CassandraConnection
import scala.language.higherKinds

object PhantomSource {
  def apply[R, T <: Table[T, R]](connection: CassandraConnection)(
    query: SelectQuery[T, R, _, _, _, _, _]
  ): DataPipeline[R, Execution.Sequential] = {
    implicit val session: Session = connection.session
    CassandraSource
      .rows(connection.session, query.executableQuery.statement())
      .map(row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5)))
  }

  def applyF[R, T <: Table[T, R], F[_], Ex <: Execution](
    connection: CassandraConnection
  )(
    query: SelectQuery[T, R, _, _, _, _, _]
  )(implicit F: Sync[F]): DataPipelineT[F, R, Ex] = {
    implicit val session: Session = connection.session
    CassandraSource
      .rowsF[F, Ex](connection.session, query.executableQuery.statement())
      .map(row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5)))
  }
}
