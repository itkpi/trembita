package com.datarootlabs.trembita.cassandra.phantom


import com.datarootlabs.trembita._
import com.datarootlabs.trembita.cassandra._
import com.datastax.driver.core.{ProtocolVersion, Session}
import com.outworkers.phantom.builder.query.SelectQuery
import com.outworkers.phantom.{Table, Row ⇒ PhantomRow}
import com.outworkers.phantom.connectors.CassandraConnection

import scala.concurrent.Future
import scala.util.Try


object PhantomSource {
  def apply[R, T <: Table[T, R]](connection: CassandraConnection)
                                (query: SelectQuery[T, R, _, _, _, _, _]): DataPipeline[R, Try, Finiteness.Finite, Execution.Sequential] = {
    implicit val session: Session = connection.session
    CassandraSource.rows(connection.session, query.executableQuery.statement())
      .map(row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5)))
  }
}

