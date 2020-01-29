package trembita

import cats.{Id, Monad}
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag
import zio._

/**
  * Typeclass for Stream or RDD-like abstractions
  * */
trait ApplicativeFlatMap[F[_]] extends Serializable {

  /** Functor.map */
  def map[A, B: ClassTag](fa: F[A])(f: A => B): F[B]

  /** Allows to create [[F]] of [[B]] applying function from [[A]] to [[Iterable]] on each element of [[F]] */
  def mapConcat[A, B: ClassTag](fa: F[A])(f: A => Iterable[B]): F[B]
  def flatten[A: ClassTag](ffa: F[Iterable[A]]): F[A] = mapConcat(ffa)(identity)

  def foreach[A](fa: F[A])(f: A => Unit): Unit = map(fa)(f)
}
trait TraverseTag[F[_]] extends Serializable {
  def traverse[Er, A, B: ClassTag](fa: F[A])(f: A => IO[Er, B]): IO[Er, F[B]]
  def sequence[Er, A: ClassTag](fga: F[IO[Er, A]]): IO[Er, F[A]] =
    traverse(fga)(identity)

  def traverse_[Er, A](fa: F[A])(f: A => IO[Er, Unit]): IO[Er, Unit] =
    traverse(fa)(f).unit
}

/**
  * trait representing specific environment on which DataPipeline can be run on.
  * For instance Sequential, Spark, Akka, etc.
  * */
trait Environment extends Serializable {
  type Repr[X]
  @implicitNotFound("""
    Pipeline you defined requires implicit typeclass for ${G} to be evaluated.
    Please ensure appropriate implicits are present in scope.
    For more information see Environment definition for your pipeline
    """)
  type Result[X]
  type ResultRepr[X] = Result[Repr[X]]

//  def absorbF[F[_], A](rfa: Result[F[A]])(implicit F: Monad[F], arrow: Result ~> F): F[A] =
//    F.flatten(arrow(rfa))

  val FlatMapResult: Monad[Result]
  val FlatMapRepr: ApplicativeFlatMap[Repr]
  val TraverseRepr: TraverseTag[Repr]

  def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit]

  def foreachF[Er, A](
      repr: Repr[A]
  )(f: A => IO[Er, Unit]): IO[Er, Unit] =
    TraverseRepr.traverse_[Er, A](repr)(f)

  def collect[A, B: ClassTag](repr: Repr[A])(pf: PartialFunction[A, B]): Repr[B]

  def distinctKeys[A: ClassTag, B: ClassTag](repr: Repr[(A, B)]): Repr[(A, B)]

  def concat[A](xs: Repr[A], ys: Repr[A]): Repr[A]

  def memoize[A: ClassTag](xs: Repr[A]): Repr[A]
}

object Environment {
  type ReprAux[Repr0[_]] = Environment { type Repr[X] = Repr0[X] }

  sealed trait Sequential extends Environment {
    final type Repr[+X]  = Vector[X]
    final type Result[X] = X

    override def collect[A, B: ClassTag](
        repr: Vector[A]
    )(pf: PartialFunction[A, B]): Vector[B] =
      repr.collect(pf)

    def fromIterable[A: ClassTag](vs: Iterable[A]): Vector[A] = vs.toVector

    def fromIterator[A: ClassTag](vs: Iterator[A]): Repr[A] = vs.toVector

    override def distinctKeys[A: ClassTag, B: ClassTag](
        repr: Repr[(A, B)]
    ): Repr[(A, B)] =
      repr.groupBy(_._1).mapValues(_.head._2).toVector

    override def concat[A](xs: Vector[A], ys: Vector[A]): Vector[A] = xs ++ ys

    def zip[A, B: ClassTag](xs: Vector[A], ys: Vector[B]): Vector[(A, B)] =
      xs.zip(ys)

    override def memoize[A: ClassTag](xs: Vector[A]): Vector[A] = xs

    override def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit] =
      repr.foreach(f)

    override val FlatMapResult: Monad[Id] = Monad[Id]

    override val FlatMapRepr: ApplicativeFlatMap[Vector] =
      new ApplicativeFlatMap[Vector] {
        def pure[A: ClassTag](a: A): Vector[A]                       = Vector(a)
        def map[A, B: ClassTag](fa: Vector[A])(f: A => B): Vector[B] = fa.map(f)
        def mapConcat[A, B: ClassTag](
            fa: Vector[A]
        )(f: A => Iterable[B]): Vector[B] =
          fa.flatMap(f)
      }
    override val TraverseRepr: TraverseTag[Vector] =
      new TraverseTag[Vector] {
        override def traverse[Er, A, B: ClassTag](fa: Vector[A])(f: A => IO[Er, B]): IO[Er, Vector[B]] = IO.collectAll(fa.map(f)).map(_.toVector)
      }
  }

  sealed trait Parallel extends Environment {
    final type Repr[+X]  = ParVector[X]
    final type Result[X] = X

    def fromVector[A: ClassTag](vs: Vector[A]): ParVector[A] = ParVector(vs: _*)

    def fromIterable[A: ClassTag](vs: Iterable[A]): Repr[A] = ParVector(vs.toVector: _*)

    def fromIterator[A: ClassTag](vs: Iterator[A]): Repr[A] = ParVector(vs.toVector: _*)

    override  def collect[A, B: ClassTag](repr: ParVector[A])(
        pf: PartialFunction[A, B]
    ): ParVector[B] = repr.collect(pf)

    override def concat[A](xs: ParVector[A], ys: ParVector[A]): ParVector[A] = xs ++ ys

    override def distinctKeys[A: ClassTag, B: ClassTag](
        repr: Repr[(A, B)]
    ): Repr[(A, B)] =
      ParVector(repr.groupBy(_._1).mapValues(_.head._2).toVector: _*)

    override val FlatMapResult: Monad[Id] = Monad[Id]

    override val FlatMapRepr: ApplicativeFlatMap[ParVector] =
      new ApplicativeFlatMap[ParVector] {
        def pure[A: ClassTag](a: A): ParVector[A] = ParVector(a)

        def mapConcat[A, B: ClassTag](
            fa: ParVector[A]
        )(f: A => Iterable[B]): ParVector[B] =
          fa.flatMap(f)

        def map[A, B: ClassTag](fa: ParVector[A])(f: A => B): ParVector[B] =
          fa.map(f)
      }

    override def memoize[A: ClassTag](xs: ParVector[A]): ParVector[A] = xs

    override val TraverseRepr: TraverseTag[ParVector] =
      new TraverseTag[ParVector] {
        override def traverse[Er, A, B: ClassTag](
            fa: ParVector[A]
        )(f: A => IO[Er, B]): IO[Er, ParVector[B]] =
          IO.collectAllPar(fa.map(f).seq).map(bs => ParVector(bs:_*))
      }

    override def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit] =
      repr.foreach(f)
  }

  implicit val Parallel: Parallel     = new Parallel   {}
  implicit val Sequential: Sequential = new Sequential {}
}
