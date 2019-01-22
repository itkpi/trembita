package trembita

import cats.Monad
import scala.language.higherKinds
import scala.reflect.ClassTag

package object caching {
  implicit class CachingOps[F[_], A, E <: Environment](private val `this`: DataPipelineT[F, A, E]) extends AnyVal {
    def cached(cacheName: String)(implicit F: Monad[F], caching: Caching[F, E, A], A: ClassTag[A]): DataPipelineT[F, A, E] =
      CachedPipelineT.make[F, A, E](`this`, cacheName)
  }
}
