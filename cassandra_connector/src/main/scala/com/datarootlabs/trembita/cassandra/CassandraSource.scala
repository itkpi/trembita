package com.datarootlabs.trembita.cassandra


import com.datarootlabs.trembita._
import com.datastax.driver.core.{Row, Session, Statement}
import scala.collection.JavaConverters._
import scala.util.Try


object CassandraSource {
  def rows(session: Session, statement: Statement): DataPipeline[Row, Try, PipelineType.Finite] =
    DataPipeline.from(session.execute(statement).iterator().asScala.toIterable)

  def apply[A](session: Session, statement: Statement)(extractor: Row => A): DataPipeline[A, Try, PipelineType.Finite] =
    rows(session, statement).map(extractor)
}
