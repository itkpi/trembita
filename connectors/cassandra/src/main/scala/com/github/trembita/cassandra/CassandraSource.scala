//package com.github.trembita.cassandra
//
//import cats.effect.Sync
//import cats.implicits._
//import com.datastax.driver.core.{Row, Session, Statement}
//import com.github.trembita.internal.StrictSource
//import com.github.trembita._
//import scala.collection.JavaConverters._
//import scala.language.higherKinds
//import scala.reflect.ClassTag
//
//object CassandraSource {
//  def rows(session: Session, statement: Statement): DataPipeline[Row, Environment.Sequential] =
//    DataPipeline.from(session.execute(statement).iterator().asScala.toIterable)
//
//  def rowsF[F[_]](session: Session, statement: Statement)(
//      implicit F: Sync[F]
//  ): DataPipelineT[F, Row, Sequential] =
//    new StrictSource[F, Row](F.delay {
//      session.execute(statement).iterator().asScala
//    }, F)
//
//  def apply[A: ClassTag](session: Session, statement: Statement)(
//      extractor: Row => A
//  ): DataPipeline[A, Environment.Sequential] =
//    rows(session, statement).mapImpl(extractor)
//
//  def applyF[F[_], A: ClassTag](session: Session, statement: Statement)(
//      extractor: Row => A
//  )(implicit F: Sync[F]): DataPipelineT[F, A, Sequential] =
//    rowsF[F](session, statement).mapImpl(extractor)
//}
