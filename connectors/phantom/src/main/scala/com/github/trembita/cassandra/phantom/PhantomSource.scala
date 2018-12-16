package com.github.trembita.cassandra.phantom

import cats.effect.Sync
import com.github.trembita._
import com.datastax.driver.core.{ProtocolVersion, Session}
import com.github.trembita.cassandra.CassandraSource
import com.outworkers.phantom.{Table, Row => PhantomRow}
import com.outworkers.phantom.builder.query.SelectQuery
import com.outworkers.phantom.connectors.CassandraConnection
import scala.language.higherKinds
import scala.reflect.ClassTag

object PhantomSource {
  def apply[R: ClassTag, T <: Table[T, R]](connection: CassandraConnection)(
    query: SelectQuery[T, R, _, _, _, _, _]
  ): DataPipeline[R, Environment.Sequential] = {
    implicit val session: Session = connection.session
    CassandraSource
      .rows(connection.session, query.executableQuery.statement())
      .mapImpl(row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5)))
  }

  def applyF[R: ClassTag, T <: Table[T, R], F[_], Ex <: Environment](
    connection: CassandraConnection
  )(
    query: SelectQuery[T, R, _, _, _, _, _]
  )(implicit F: Sync[F]): DataPipelineT[F, R, Sequential] = {
    implicit val session: Session = connection.session
    CassandraSource
      .rowsF[F](connection.session, query.executableQuery.statement())
      .mapImpl(row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5)))
  }
}
