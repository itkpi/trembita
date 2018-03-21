package com.datarootlabs.trembita.cassandra


import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._
import com.datastax.driver.core.{Row, Session, Statement}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext


protected[trembita]
class CassandraSource(session: Session, statement: Statement) extends SeqSource[Row] {
  override def force: Iterable[Row] = this.iterator.toIterable
  override def par(implicit ec: ExecutionContext): ParDataPipeline[Row] = new ParSource[Row](this.force)
  override def iterator: Iterator[Row] = {
    val result = session.execute(statement)
    result.iterator().asScala
  }
}

object CassandraSource {
  def rows(session: Session, statement: Statement): DataPipeline[Row] = new CassandraSource(session, statement)
  def apply[A](session: Session, statement: Statement)(extractor: Row => A): DataPipeline[A] =
    rows(session, statement).map(extractor)
}
