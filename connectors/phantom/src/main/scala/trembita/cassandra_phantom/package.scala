package trembita

import cats.{~>, Id, Monad}
import cats.effect.Sync
import com.datastax.driver.core.{ProtocolVersion, Session}
import trembita.cassandra._
import com.outworkers.phantom.{Table, Row => PhantomRow}
import com.outworkers.phantom.builder.query.SelectQuery
import com.outworkers.phantom.connectors.CassandraConnection
import scala.language.higherKinds
import scala.reflect.ClassTag

package object cassandra_phantom {
  class cassandraPhantomInputDsl[F[_]](val `this`: (Id ~> F, Monad[F])) extends AnyVal {
    def rows[R: ClassTag, T <: Table[T, R]](connection: CassandraConnection)(
        query: SelectQuery[T, R, _, _, _, _, _]
    ): DataPipeline[R, Environment.Sequential] = {
      implicit val session: Session = connection.session
      Input.cassandraTable
        .fromRows(connection.session, query.executableQuery.statement())(
          row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5))
        )
    }

    def applyF[R: ClassTag, T <: Table[T, R]](
        connection: CassandraConnection
    )(
        query: SelectQuery[T, R, _, _, _, _, _]
    )(implicit F: Sync[F]): DataPipelineT[F, R, Sequential] = {
      implicit val session: Session = connection.session
      Input
        .cassandraTableSync[F]
        .fromRows(connection.session, query.executableQuery.statement())(
          row => query.fromRow(new PhantomRow(row, ProtocolVersion.V5))
        )
    }
  }

  implicit class CassandraPhantomInput(private val self: Input.type) extends AnyVal {
    def cassandraPhantom: cassandraPhantomInputDsl[Id] = new cassandraPhantomInputDsl[Id]((identityK[Id], Monad[Id]))
    def cassandraPhantomF[F[_]](implicit F: Monad[F]): cassandraPhantomInputDsl[F] = new cassandraPhantomInputDsl[F]((idTo[F], F))
    def cassandraPhantomSync[F[_]](implicit F: Sync[F]): cassandraPhantomInputDsl[F] = new cassandraPhantomInputDsl[F]((λ[Id[?] ~> F[?]](F.delay(_)), F))
  }
}
