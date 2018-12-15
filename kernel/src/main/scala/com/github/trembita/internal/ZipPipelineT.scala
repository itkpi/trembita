package com.github.trembita.internal

import cats.Monad
import com.github.trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag

protected[trembita] class ZipPipelineT[F[_], A, B, Ex <: Execution](
  left: DataPipelineT[F, A, Ex],
  right: DataPipelineT[F, B, Ex]
)(implicit A: ClassTag[A], F: Monad[F], B: ClassTag[B])
    extends SeqSource[F, (A, B), Ex](F) {
  protected[trembita] def evalFunc[C >: (A, B)](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(left.evalFunc[A](Ex)) { leftRepr =>
      F.map(right.evalFunc[B](Ex))(
        rightRepr => Ex.zip(leftRepr, rightRepr).asInstanceOf[Ex.Repr[C]]
      )
    }
}

protected[trembita] class ConcatPipelineT[F[_], A, Ex <: Execution](
  left: DataPipelineT[F, A, Ex],
  right: DataPipelineT[F, A, Ex]
)(implicit F: Monad[F], A: ClassTag[A])
    extends SeqSource[F, A, Ex](F) {
  protected[trembita] def evalFunc[B >: A](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.flatMap(left.evalFunc[A](Ex)) { leftRepr =>
      F.map(right.evalFunc[A](Ex))(
        rightRepr => Ex.concat(leftRepr, rightRepr).asInstanceOf[Ex.Repr[B]]
      )
    }
}

protected[trembita] class JoinPipelineT[F[_], A, B, Ex <: Execution](
  left: DataPipelineT[F, A, Ex],
  right: DataPipelineT[F, B, Ex],
  on: (A, B) => Boolean
)(implicit A: ClassTag[A],
  F: Monad[F],
  B: ClassTag[B],
  canJoin: CanJoin[Ex#Repr])
    extends SeqSource[F, (A, B), Ex](F) {
  protected[trembita] def evalFunc[C >: (A, B)](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(left.evalFunc[A](Ex)) { leftRepr =>
      F.map(right.evalFunc[B](Ex))(
        rightRepr =>
          canJoin.join(leftRepr, rightRepr)(on).asInstanceOf[Ex.Repr[C]]
      )
    }
}

protected[trembita] class JoinLeftPipelineT[F[_], A, B, Ex <: Execution](
  left: DataPipelineT[F, A, Ex],
  right: DataPipelineT[F, B, Ex],
  on: (A, B) => Boolean
)(implicit A: ClassTag[A],
  F: Monad[F],
  B: ClassTag[B],
  canJoin: CanJoin[Ex#Repr])
    extends SeqSource[F, (A, Option[B]), Ex](F) {
  protected[trembita] def evalFunc[C >: (A, Option[B])](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(left.evalFunc[A](Ex)) { leftRepr =>
      F.map(right.evalFunc[B](Ex))(
        rightRepr =>
          canJoin.joinLeft(leftRepr, rightRepr)(on).asInstanceOf[Ex.Repr[C]]
      )
    }
}

protected[trembita] class JoinRightPipelineT[F[_], A, B, Ex <: Execution](
  left: DataPipelineT[F, A, Ex],
  right: DataPipelineT[F, B, Ex],
  on: (A, B) => Boolean
)(implicit A: ClassTag[A],
  F: Monad[F],
  B: ClassTag[B],
  canJoin: CanJoin[Ex#Repr])
    extends SeqSource[F, (Option[A], B), Ex](F) {
  protected[trembita] def evalFunc[C >: (Option[A], B)](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(left.evalFunc[A](Ex)) { leftRepr =>
      F.map(right.evalFunc[B](Ex))(
        rightRepr =>
          canJoin.joinRight(leftRepr, rightRepr)(on).asInstanceOf[Ex.Repr[C]]
      )
    }
}
