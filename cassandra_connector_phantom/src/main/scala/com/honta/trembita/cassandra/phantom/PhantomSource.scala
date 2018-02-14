package com.honta.trembita.cassandra.phantom


import com.datastax.driver.core.{ProtocolVersion, Session}
import com.honta.trembita.LazyList
import com.honta.trembita.cassandra.CassandraSource
import com.outworkers.phantom.builder.query.SelectQuery
import com.outworkers.phantom.{Table, Row => PhantomRow}
import com.outworkers.phantom.connectors.CassandraConnection

object PhantomSource {
  def apply[R, T <: Table[T, R]](connection: CassandraConnection)
                                (query: SelectQuery[T, R, _, _, _, _, _]): LazyList[R] = {
    implicit val session: Session = connection.session
    CassandraSource.rows(connection.session, query.executableQuery.statement())
      .map(row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5)))
  }
}

