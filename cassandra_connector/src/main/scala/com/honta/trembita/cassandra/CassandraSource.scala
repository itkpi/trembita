package com.honta.trembita.cassandra


import com.honta.trembita.internal._
import com.datastax.driver.core.{Row, Session, Statement}
import com.honta.trembita.LazyList
import com.honta.trembita.parallel.{ParLazyList, ParSource}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext


protected[trembita] class CassandraSource(session: Session, statement: Statement) extends SeqSource[Row] {
  override def force: Iterable[Row] = this.iterator.toIterable
  override def par(implicit ec: ExecutionContext): ParLazyList[Row] = new ParSource[Row](this.force)
  override def iterator: Iterator[Row] = {
    val result = session.execute(statement)
    result.iterator().asScala
  }
}

object CassandraSource {
  def rows(session: Session, statement: Statement): LazyList[Row] = new CassandraSource(session, statement)
  def apply[A](session: Session, statement: Statement)(extractor: Row => A): LazyList[A] =
    rows(session, statement).map(extractor)
}
