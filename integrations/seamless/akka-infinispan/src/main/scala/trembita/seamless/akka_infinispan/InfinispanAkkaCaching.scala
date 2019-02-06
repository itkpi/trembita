package trembita.seamless.akka_infinispan

import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.stage._
import cats.Monad
import cats.effect.Sync
import org.infinispan.commons.api.BasicCache
import trembita.Environment
import trembita.akka_streams.Akka
import trembita.caching._
import trembita.caching.infinispan._
import trembita.util.LiftFuture
import cats.implicits._
import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.reflect.ClassTag

class InfinispanAkkaCaching[F[_], Mat, A: ClassTag](
    cache: BasicCache[String, (Vector[A], Mat)],
    expirationTimeout: ExpirationTimeout,
    F: Sync[F],
    liftFuture: LiftFuture[F]
)(implicit fCtg: ClassTag[F[_]], mat: Materializer, ec: ExecutionContext)
    extends Caching[F, Akka[Mat], A] {
  protected implicit def monad: Monad[F] = F

  protected def timeout: ExpirationTimeout = expirationTimeout

  protected def cacheRepr(cacheName: String, repr: Source[A, Mat]): F[Unit] =
    F.suspend {
      liftFuture {
        val (matValue, futureVs) = repr
          .toMat(Sink.collection[A, Vector[A]])(Keep.both)
          .run()
        futureVs.map(_ -> matValue)
      }.flatMap {
        case (vs, matValue) =>
          liftFuture {
            cache.putIfAbsentAsync(cacheName, vs -> matValue, expirationTimeout.duration.length, expirationTimeout.duration.unit).toScalaOpt
          }
      }.void
    }

  protected def getFromCache(cacheName: String): F[Option[Source[A, Mat]]] =
    F.suspend {
        liftFuture {
          cache.getAsync(cacheName).toScalaOpt
        }
      }
      .map(_.map { case (vs, matValue) => Source(vs).mapMaterializedValue(_ => matValue) })

  def stop(): F[Unit] = F.delay(cache.stop())

}

object InfinispanAkkaCaching {
  def apply[F[_]: Sync: LiftFuture, Mat: ClassTag, A: ClassTag](
      cacheF: F[BasicCache[String, (Vector[A], Mat)]],
      expirationTimeout: ExpirationTimeout
  )(implicit fCtg: ClassTag[F[_]], mat: Materializer, ec: ExecutionContext): F[Caching[F, Akka[Mat], A]] =
    cacheF.flatMap { cache =>
      Sync[F].delay(cache.start()) *>
        (new InfinispanAkkaCaching[F, Mat, A](cache, expirationTimeout, Sync[F], LiftFuture[F]): Caching[F, Akka[Mat], A])
          .pure[F]
    }
}
