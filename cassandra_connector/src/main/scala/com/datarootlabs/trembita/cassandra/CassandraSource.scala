package com.datarootlabs.trembita.cassandra


import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._
import com.datastax.driver.core.{Row, Session, Statement}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext


protected[trembita]
class CassandraSource(session: Session, statement: Statement) extends SeqSource[Row] {
  override def eval: Iterable[Row] = this.iterator.toIterable
  override def par(implicit ec: ExecutionContext): ParDataPipeline[Row] = new ParSource[Row](this.eval)
  override def iterator: Iterator[Row] = {
    val result = session.execute(statement)
    result.iterator().asScala
  }

  override def :+[BB >: Row](elem: BB): DataPipeline[BB] = new StrictSource(this.eval ++ Some(elem))
  override def ++[BB >: Row](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(this.eval ++ that.eval)
}

object CassandraSource {
  def rows(session: Session, statement: Statement): DataPipeline[Row] = new CassandraSource(session, statement)
  def apply[A](session: Session, statement: Statement)(extractor: Row => A): DataPipeline[A] =
    rows(session, statement).map(extractor)
}
