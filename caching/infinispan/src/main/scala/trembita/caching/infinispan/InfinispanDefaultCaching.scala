package trembita.caching.infinispan

import cats.Monad
import cats.effect.Sync
import trembita.Environment
import trembita.caching.{Caching, ExpirationTimeout}

import scala.compat.java8.FutureConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag
import cats.syntax.all._
import trembita.util.LiftFuture
import org.infinispan.commons.api.BasicCache

class InfinispanDefaultCaching[F[_], E <: Environment: ClassTag, A: ClassTag](
    cache: BasicCache[String, E#Repr[A]],
    expirationTimeout: ExpirationTimeout,
    F: Sync[F],
    liftFuture: LiftFuture[F]
)(implicit fCtg: ClassTag[F[_]])
    extends Caching[F, E, A] {

  private val E = implicitly[ClassTag[E]]
  private val A = implicitly[ClassTag[A]]

  implicit protected val monad: Monad[F]   = F
  protected val timeout: ExpirationTimeout = expirationTimeout

  protected def cacheRepr(cacheName: String, repr: E#Repr[A]): F[Unit] = F.suspend {
    liftFuture {
      cache
        .putIfAbsentAsync(cacheName, repr, timeout.duration.length, timeout.duration.unit)
        .toScalaOpt
    }.void
  }

  protected def getFromCache(cacheName: String): F[Option[E#Repr[A]]] =
    F.suspend {
      liftFuture {
        cache.getAsync(cacheName).toScalaOpt
      }
    }

  def stop(): F[Unit] = F.delay(cache.stop())
}

object InfinispanDefaultCaching {
  def apply[F[_]: Sync: LiftFuture, E <: Environment: ClassTag, A: ClassTag](
      cacheF: F[BasicCache[String, E#Repr[A]]],
      expirationTimeout: ExpirationTimeout
  )(implicit fCtg: ClassTag[F[_]]): F[Caching[F, E, A]] =
    cacheF.flatMap { cache =>
      Sync[F].delay(cache.start()) *>
        (new InfinispanDefaultCaching[F, E, A](cache, expirationTimeout, Sync[F], LiftFuture[F]): Caching[F, E, A]).pure[F]
    }
}
