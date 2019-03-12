package trembita.internal

import cats._
import trembita._
import trembita.operations.{CanGroupBy, CanGroupByOrdered}
import scala.annotation.unchecked.uncheckedVariance
import scala.language.higherKinds
import scala.reflect.ClassTag

/**
  * Special case for [[BiDataPipelineT]]
  * representing a pipeline of tuples
  * with UNIQUE keys
  *
  * @tparam K - key
  * @tparam V - value
  **/
trait BiMapPipelineT[F[_], Er, K, V, E <: Environment] extends BiDataPipelineT[F, Er, (K, V), E] {

  /**
    * Applies mapping function only for the values
    *
    * @tparam W - resulting value type
    * @param f - transformation function
    * @return - a pipeline with the same key and transformed values
    **/
  def mapValues[W: ClassTag](f: V => W)(implicit F: MonadError[F, Er]): BiMapPipelineT[F, Er, K, W, E]

  /**
    * Returns only those ([[K]], [[V]]) pairs
    * that satisfies given predicate
    *
    * @param p - predicate
    * @return - filtered [[BiMapPipelineT]]
    **/
  def filterKeys(p: K => Boolean)(
      implicit F: MonadError[F, Er]
  ): BiMapPipelineT[F, Er, K, V, E]

  /** @return - pipeline with keys only */
  def keys(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, K, E]

  /** @return - pipeline with values only */
  def values(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, V, E]
}

/**
  * Sequential implementation of [[BiMapPipelineT]]
  *
  * @tparam K -key
  * @tparam V - value
  * @param source - (K, V) pair pipeline
  **/
protected[trembita] case class BaseMapPipelineT[F[_], Er: ClassTag, K, V, E <: Environment](
    source: BiDataPipelineT[F, Er, (K, V), E],
    F: MonadError[F, Er]
)(implicit K: ClassTag[K], V: ClassTag[V])
    extends SeqSource[F, Er, (K, V), E](F)
    with BiMapPipelineT[F, Er, K, V, E] {

  def mapValues[W: ClassTag](f: V => W)(implicit F: MonadError[F, Er]): BiMapPipelineT[F, Er, K, W, E @uncheckedVariance] =
    new BaseMapPipelineT[F, Er, K, W, E](source.mapImpl { case (k, v) => k -> f(v) }, F)

  def filterKeys(
      p: K => Boolean
  )(implicit F: MonadError[F, Er]): BiMapPipelineT[F, Er, K, V, E] =
    new BaseMapPipelineT[F, Er, K, V, E](source.collectImpl {
      case (k, v) if p(k) => (k, v)
    }, F)

  def keys(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, K, E] =
    new MappingPipelineT[F, Er, (K, V), K, E](_._1, this)(F)

  def values(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, V, E] =
    new MappingPipelineT[F, Er, (K, V), V, E](_._2, this)(F)

  protected[trembita] def evalFunc[B >: (K, V)](E: E)(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
    F.map(
      source
        .evalFunc[(K, V)](E)
    ) { vs =>
      val (errors, values)                = E.FlatMapRepr.separate(vs)
      val res: E.Repr[Either[Er, (K, V)]] = E.unite(errors, E.distinctKeys(values))
      res.asInstanceOf[E.Repr[Either[Er, B]]]
    }
}

/**
  * A [[BiDataPipelineT]]
  * been grouped by some criteria
  *
  * @tparam K - grouping criteria type
  * @tparam V - value
  * @param f - grouping function
  **/
object GroupByPipelineT {
  def make[F[_], Er: ClassTag, K, V, E <: Environment](
      f: V => K,
      source: BiDataPipelineT[F, Er, V, E],
      F: MonadError[F, Er],
      canGroupBy: CanGroupBy[E#Repr]
  )(
      implicit K: ClassTag[K],
      V: ClassTag[V]
  ): BiDataPipelineT[F, Er, (K, Iterable[V]), E] = new SeqSource[F, Er, (K, Iterable[V]), E](F) {
    protected[trembita] def evalFunc[B >: (K, Iterable[V])](E: E)(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
      F.map(
        source
          .evalFunc[V](E)
      ) { vs =>
        val (errors, values) = E.FlatMapRepr.separate(vs)
        val res: E.Repr[Either[Er, (K, Iterable[V])]] =
          E.unite(errors, canGroupBy.groupBy(values.asInstanceOf[E#Repr[V]])(f).asInstanceOf[E.Repr[(K, Iterable[V])]])
        res.asInstanceOf[E.Repr[Either[Er, B]]]
      }

    override def toString: String = s"GroupByPipelineT($f, $source, $f, $canGroupBy)($K, $V)"
  }
}

/**
  * A [[BiDataPipelineT]]
  * been grouped by some criteria
  *
  * @tparam K - grouping criteria type
  * @tparam V - value
  * @param f - grouping function
  **/
object GroupByOrderedPipelineT {
  def make[F[_], Er: ClassTag, K, V, E <: Environment](
      f: V => K,
      source: BiDataPipelineT[F, Er, V, E],
      F: MonadError[F, Er],
      canGroupBy: CanGroupByOrdered[E#Repr]
  )(
      implicit K: ClassTag[K],
      V: ClassTag[V],
      ordering: Ordering[K]
  ): BiDataPipelineT[F, Er, (K, Iterable[V]), E] =
    new SeqSource[F, Er, (K, Iterable[V]), E](F) {
      protected[trembita] def evalFunc[B >: (K, Iterable[V])](E: E)(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
        F.map(
          source
            .evalFunc[V](E)
        ) { vs =>
          val (errors, values) = E.FlatMapRepr.separate(vs)
          val res: E.Repr[Either[Er, (K, Iterable[V])]] =
            E.unite(errors, canGroupBy.groupBy(values.asInstanceOf[E#Repr[V]])(f).asInstanceOf[E.Repr[(K, Iterable[V])]])
          res.asInstanceOf[E.Repr[Either[Er, B]]]
        }
      override def toString: String = s"GroupByOrderedPipelineT($f, $source, $f, $canGroupBy)($K, $V, $ordering)"
    }
}
