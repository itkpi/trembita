package trembita.internal

import cats.{Monad, MonadError}
import trembita._
import trembita.operations.{CanJoin, CanZip}

import scala.annotation.unchecked.uncheckedVariance
import scala.language.higherKinds
import scala.reflect.ClassTag

protected[trembita] class ZipPipelineT[F[_], Er: ClassTag, A, B, E <: Environment](
    left: BiDataPipelineT[F, Er, A, E],
    right: BiDataPipelineT[F, Er, B, E],
    canZip: CanZip[E#Repr]
)(implicit A: ClassTag[A], F: MonadError[F, Er @uncheckedVariance], B: ClassTag[B])
    extends SeqSource[F, Er, (A, B), E](F) {
  protected[trembita] def evalFunc[C >: (A, B)](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, C]]] =
    F.flatMap(left.evalFunc[A](E)) { leftRepr =>
      val (leftErrors, leftValues) = E.FlatMapRepr.separate(leftRepr)
      F.map(right.evalFunc[B](E)) { rightRepr =>
        val (rightErrors, rightValues) = E.FlatMapRepr.separate(rightRepr)
        val zippedValues               = canZip.zip(leftValues.asInstanceOf[E#Repr[A]], rightValues.asInstanceOf[E#Repr[B]]).asInstanceOf[E.Repr[(A, C)]]
        E.unite(E.concat(leftErrors, rightErrors), zippedValues).asInstanceOf[E.Repr[Either[Er, C]]]
      }
    }

  override def toString: String = s"ZipPipelineT($left, $right, $canZip)($A, $F, $B)"
}

protected[trembita] case class ConcatPipelineT[F[_], Er: ClassTag, A, E <: Environment](
    left: BiDataPipelineT[F, Er, A, E],
    right: BiDataPipelineT[F, Er, A, E]
)(implicit F: MonadError[F, Er @uncheckedVariance], A: ClassTag[A])
    extends SeqSource[F, Er, A, E](F) {
  protected[trembita] def evalFunc[B >: A](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
    F.flatMap(left.evalFunc[A](E)) { leftRepr =>
      val (leftErrors, leftValues) = E.FlatMapRepr.separate(leftRepr)
      F.map(right.evalFunc[A](E)) { rightRepr =>
        val (rightErrors, rightValues) = E.FlatMapRepr.separate(rightRepr)
        val concatedValues             = E.concat(leftValues, rightValues)
        E.unite(E.concat(leftErrors, rightErrors), concatedValues).asInstanceOf[E.Repr[Either[Er, B]]]
      }
    }

  override protected[trembita] def handleErrorImpl[Err >: Er, B >: A: ClassTag](f: Err => B)(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, E] =
    new ConcatPipelineT(left.handleErrorImpl[Err, B](f), right.handleErrorImpl[Err, B](f))(
      implicitly[ClassTag[Er]].asInstanceOf[ClassTag[Err]],
      F,
      implicitly[ClassTag[B]]
    )
  override protected[trembita] def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => F[B])(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, E] =
    new ConcatPipelineT(left.handleErrorWithImpl[Err, B](f), right.handleErrorWithImpl[Err, B](f))(
      implicitly[ClassTag[Er]].asInstanceOf[ClassTag[Err]],
      implicitly,
      implicitly
    )
  override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      f: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E] =
    new ConcatPipelineT[F, Er2, A, E](left.transformErrorImpl[Err, Er2](f), right.transformErrorImpl[Err, Er2](f))(
      implicitly[ClassTag[Er2]],
      F,
      A
    )
}

protected[trembita] case class JoinPipelineT[F[_], Er: ClassTag, A, B, E <: Environment](
    left: BiDataPipelineT[F, Er, A, E],
    right: BiDataPipelineT[F, Er, B, E],
    on: (A, B) => Boolean
)(implicit A: ClassTag[A], F: MonadError[F, Er @uncheckedVariance], B: ClassTag[B], canJoin: CanJoin[E#Repr])
    extends SeqSource[F, Er, (A, B), E](F) {
  protected[trembita] def evalFunc[C >: (A, B)](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, C]]] =
    F.flatMap(left.evalFunc[A](E)) { leftRepr =>
      val (leftErrors, leftValues) = E.FlatMapRepr.separate(leftRepr)
      F.map(right.evalFunc[B](E)) { rightRepr =>
        val (rightErrors, rightValues) = E.FlatMapRepr.separate(rightRepr)
        val joined                     = canJoin.join(leftValues.asInstanceOf[E#Repr[A]], rightValues.asInstanceOf[E#Repr[B]])(on).asInstanceOf[E.Repr[(A, B)]]
        E.unite(E.concat(leftErrors, rightErrors), joined).asInstanceOf[E.Repr[Either[Er, C]]]
      }
    }
}

protected[trembita] case class JoinLeftPipelineT[F[_], Er: ClassTag, A, B, E <: Environment](
    left: BiDataPipelineT[F, Er, A, E],
    right: BiDataPipelineT[F, Er, B, E],
    on: (A, B) => Boolean
)(implicit A: ClassTag[A], F: MonadError[F, Er @uncheckedVariance], B: ClassTag[B], canJoin: CanJoin[E#Repr])
    extends SeqSource[F, Er, (A, Option[B]), E](F) {
  protected[trembita] def evalFunc[C >: (A, Option[B])](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, C]]] =
    F.flatMap(left.evalFunc[A](E)) { leftRepr =>
      val (leftErrors, leftValues) = E.FlatMapRepr.separate(leftRepr)
      F.map(right.evalFunc[B](E)) { rightRepr =>
        val (rightErrors, rightValues) = E.FlatMapRepr.separate(rightRepr)
        val joined =
          canJoin.joinLeft(leftValues.asInstanceOf[E#Repr[A]], rightValues.asInstanceOf[E#Repr[B]])(on).asInstanceOf[E.Repr[(A, Option[B])]]
        E.unite(E.concat(leftErrors, rightErrors), joined).asInstanceOf[E.Repr[Either[Er, C]]]
      }
    }
}

protected[trembita] case class JoinRightPipelineT[F[_], Er: ClassTag, A, B, E <: Environment](
    left: BiDataPipelineT[F, Er, A, E],
    right: BiDataPipelineT[F, Er, B, E],
    on: (A, B) => Boolean
)(implicit A: ClassTag[A], F: MonadError[F, Er @uncheckedVariance], B: ClassTag[B], canJoin: CanJoin[E#Repr])
    extends SeqSource[F, Er, (Option[A], B), E](F) {
  protected[trembita] def evalFunc[C >: (Option[A], B)](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, C]]] =
    F.flatMap(left.evalFunc[A](E)) { leftRepr =>
      val (leftErrors, leftValues) = E.FlatMapRepr.separate(leftRepr)
      F.map(right.evalFunc[B](E)) { rightRepr =>
        val (rightErrors, rightValues) = E.FlatMapRepr.separate(rightRepr)
        val joined =
          canJoin.joinLeft(leftValues.asInstanceOf[E#Repr[A]], rightValues.asInstanceOf[E#Repr[B]])(on).asInstanceOf[E.Repr[(Option[A], B)]]
        E.unite(E.concat(leftErrors, rightErrors), joined).asInstanceOf[E.Repr[Either[Er, C]]]
      }
    }
}
