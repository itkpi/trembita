package trembita

import cats.{~>, Id, Monad}
import cats.effect.Sync
import com.datastax.driver.core.{Row, Session, Statement}
import trembita.internal.StrictSource
import scala.collection.JavaConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag

package object cassandra {
  class cassandraInputDslF[F[_]](val `this`: (Id ~> F, Monad[F])) extends AnyVal {
    def rows(session: Session, statement: Statement): DataPipelineT[F, Row, Sequential] =
      new StrictSource[F, Row](`this`._1(session.execute(statement).iterator().asScala), `this`._2)

    def fromRows[A: ClassTag](session: Session, statement: Statement)(extractor: Row => A): DataPipelineT[F, A, Sequential] =
      rows(session, statement).mapImpl(extractor)(implicitly, `this`._2)
  }

  implicit class CassandraInput(private val self: Input.type) extends AnyVal {
    def cassandraTable: cassandraInputDslF[Id] = new cassandraInputDslF[Id]((identityK[Id], Monad[Id]))

    def cassandraTableF[F[_]](implicit F: Monad[F]): cassandraInputDslF[F] = new cassandraInputDslF[F]((idTo[F], F))

    def cassandraTableSync[F[_]](implicit F: Sync[F]): cassandraInputDslF[F] =
      new cassandraInputDslF[F]((λ[Id[?] ~> F[?]](F.delay(_)), F))
  }

}
