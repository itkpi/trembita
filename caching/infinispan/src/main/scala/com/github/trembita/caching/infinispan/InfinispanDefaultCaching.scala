package com.github.trembita.caching.infinispan

import cats.Monad
import cats.effect.Sync
import org.infinispan.manager.DefaultCacheManager
import com.github.trembita.Environment
import com.github.trembita.caching.{Caching, ExpirationTimeout}
import org.infinispan.metadata.EmbeddedMetadata

import scala.compat.java8.FutureConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag
import cats.syntax.all._
import org.infinispan.configuration.cache.Configuration

class InfinispanDefaultCaching[F[_], E <: Environment: ClassTag, A: ClassTag](
    cacheManager: DefaultCacheManager,
    expirationTimeout: ExpirationTimeout,
    F: Sync[F],
    liftFuture: LiftFuture[F]
)(implicit fCtg: ClassTag[F[_]])
    extends Caching[F, E, A] {

  private val E               = implicitly[ClassTag[E]]
  private val A               = implicitly[ClassTag[A]]
  private val globalCacheName = s"trembita-cache-$fCtg-$E-$A"
  private val cache           = cacheManager.getCache[String, E#Repr[A]](globalCacheName).getAdvancedCache

  implicit protected val monad: Monad[F]   = F
  protected val timeout: ExpirationTimeout = expirationTimeout

  protected def cacheRepr(cacheName: String, repr: E#Repr[A]): F[Unit] = F.suspend {
    liftFuture {
      cache
        .putIfAbsentAsync(cacheName, repr, new EmbeddedMetadata.Builder().lifespan(timeout.duration.length, timeout.duration.unit).build())
        .toScala
    }.void
  }

  protected def getFromCache(cacheName: String): F[Option[E#Repr[A]]] =
    F.suspend {
        liftFuture {
          cache.getCacheEntryAsync(cacheName).toScala
        }
      }
      .map(Option(_).map(_.getValue))

  def stop(): F[Unit] = F.delay(cacheManager.stop())
}

object InfinispanDefaultCaching {
  def apply[F[_]: Sync: LiftFuture, E <: Environment: ClassTag, A: ClassTag](
      configuration: Configuration,
      expirationTimeout: ExpirationTimeout
  )(implicit fCtg: ClassTag[F[_]]): F[Caching[F, E, A]] =
    Sync[F].delay(new DefaultCacheManager(configuration)).flatMap { cacheManager =>
      Sync[F].delay(cacheManager.start()) *>
        (new InfinispanDefaultCaching[F, E, A](cacheManager, expirationTimeout, Sync[F], LiftFuture[F]): Caching[F, E, A]).pure[F]
    }
}
