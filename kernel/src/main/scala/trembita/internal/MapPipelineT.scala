package trembita.internal

import cats.syntax.either._
import trembita._
import trembita.operations.{CanGroupBy, CanGroupByOrdered}
import zio._
import scala.reflect.ClassTag

/**
 * Special case for [[DataPipelineT]]
 * representing a pipeline of tuples
 * with UNIQUE keys
 *
 * @tparam K - key
 * @tparam V - value
 **/
trait MapPipelineT[Er, K, V, Ex <: Environment] extends DataPipelineT[Er, (K, V), Ex] {

  /**
   * Applies mapping function only for the values
   *
   * @tparam W - resulting value type
   * @param f - transformation function
   * @return - a pipeline with the same key and transformed values
   **/
  def mapValues[W: ClassTag](f: V => W): MapPipelineT[Er, K, W, Ex]

  /**
   * Returns only those ([[K]], [[V]]) pairs
   * that satisfies given predicate
   *
   * @param p - predicate
   * @return - filtered [[MapPipelineT]]
   **/
  def filterKeys(p: K => Boolean): MapPipelineT[Er, K, V, Ex]

  /** @return - pipeline with keys only */
  def keys: DataPipelineT[Er, K, Ex]

  /** @return - pipeline with values only */
  def values: DataPipelineT[Er, V, Ex]
}

/**
 * Sequential implementation of [[MapPipelineT]]
 *
 * @tparam K -key
 * @tparam V - value
 * @param source - (K, V) pair pipeline
 **/
@internalAPI
protected[trembita] class BaseMapPipelineT[Er, K, V, Ex <: Environment](
                                                                         source: DataPipelineT[Er, (K, V), Ex]
                                                                       )(implicit K: ClassTag[K], V: ClassTag[V])
  extends SeqSource[Er, (K, V), Ex]
    with MapPipelineT[Er, K, V, Ex] {

  override def mapValues[W: ClassTag](f: V => W): MapPipelineT[Er, K, W, Ex] =
    new BaseMapPipelineT[Er, K, W, Ex](source.mapImpl { case (k, v) => k -> f(v) })

  override def filterKeys(
                           p: K => Boolean
                         ): MapPipelineT[Er, K, V, Ex] =
    new BaseMapPipelineT[Er, K, V, Ex](source.collectImpl {
      case (k, v) if p(k) => (k, v)
    })

  override def keys: DataPipelineT[Er, K, Ex] =
    new MappingPipelineT[Er, (K, V), K, Ex](_._1, this)

  override def values: DataPipelineT[Er, V, Ex] =
    new MappingPipelineT[Er, (K, V), V, Ex](_._2, this)

  override protected[trembita] def evalFunc[EE >: Er, B >: (K, V)](Ex: Ex): UIO[Ex.Repr[Either[EE, B]]] =
    source
      .evalFunc[Er, (K, V)](Ex)
      .map { vs =>
        val distinct = Ex.distinctKeys[Either[EE, K], Either[EE, V]](
          Ex.FlatMapRepr.map(vs) {
            case Left(e)       => e.asLeft      -> e.asLeft
            case Right((k, v)) => k.asRight[EE] -> v.asRight
          }
        )

        Ex.FlatMapRepr.map(distinct) {
          case (Left(e), _)         => e.asLeft
          case (_, Left(e))         => e.asLeft
          case (Right(k), Right(v)) => (k, v).asInstanceOf[B].asRight
        }
      }
}

/**
 * A [[DataPipelineT]]
 * been grouped by some criteria
 **/
@internalAPI
object GroupByPipelineT {

  /**
   *
   * @tparam K - grouping criteria type
   * @tparam V - value
   * @param f - grouping function
   * */
  def make[Er, K, V, Ex <: Environment](
                                         f: V => K,
                                         source: DataPipelineT[Er, V, Ex],
                                         canGroupBy: CanGroupBy[Ex#Repr]
                                       )(
                                         implicit K: ClassTag[K],
                                         V: ClassTag[V]
                                       ): DataPipelineT[Er, (K, Iterable[V]), Ex] =
    new SeqSource[Er, (K, Iterable[V]), Ex] {
      protected[trembita] def evalFunc[EE >: Er, B >: (K, Iterable[V])](Ex: Ex): UIO[Ex.Repr[Either[EE, B]]] =
        source
          .evalFunc[Er, V](Ex)
          .map(
            vs =>
              canGroupBy.groupBy[K, Either[Er, V]](vs) {
                case Left(e)  => ???
                case Right(v) => f(v)
              }
          )
    }
}

/**
 * A [[DataPipelineT]]
 * been grouped by some criteria
 **/
@internalAPI
object GroupByOrderedPipelineT {

  /**
   * @tparam K - grouping criteria type
   * @tparam V - value
   * @param f - grouping function
   * */
  def make[Er, K, V, Ex <: Environment](
                                         f: V => K,
                                         source: DataPipelineT[Er, V, Ex],
                                         canGroupBy: CanGroupByOrdered[Ex#Repr]
                                       )(
                                         implicit K: ClassTag[K],
                                         V: ClassTag[V],
                                         ordering: Ordering[K]
                                       ): DataPipelineT[Er, (K, Iterable[V]), Ex] =
    new SeqSource[Er, (K, Iterable[V]), Ex] {
      override protected[trembita] def evalFunc[EE >: Er, B >: (K, Iterable[V])](Ex: Ex): UIO[Ex.Repr[Either[EE, B]]] =
        source
          .evalFunc[Er, V](Ex)
          .map(
            vs => ???
          )
    }
}
