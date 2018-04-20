package com.datarootlabs.trembita.mutable

import scala.language.higherKinds
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}
import cats._
import cats.implicits._


class StateEff[M[_], A](@volatile private var state: M[A])(implicit M: MonadError[M, Throwable]) {
  private val lock: ReadWriteLock = new ReentrantReadWriteLock()

  private def writeLock[B](block: ⇒ B): B = {
    lock.writeLock().lock()
    val a: B = block
    lock.writeLock().unlock()
    a
  }

  private def readLock[B](block: ⇒ B): B = {
    lock.readLock().lock()
    val a: B = block
    lock.readLock().unlock()
    a
  }

  def mutate(mutation: A ⇒ A): M[Either[Throwable, Unit]] =
    mutateMapSync(mutation(_) → {})

  def mutateMapSync[B](mutation: A ⇒ (A, B)): M[Either[Throwable, B]] = writeLock {
    val newState: M[Either[Throwable, (A, B)]] = M.attempt { state.map(mutation) }
    state = newState.flatMap {
      case Right((a, _)) ⇒ M.pure(a)
      case Left(_)       ⇒ state
    }

    newState.map(_.right.map(_._2))
  }

  def mutateMap[B](mutation: A ⇒ (A, B)): M[Either[Throwable, B]] = {
    val newStateM = readLock {
      M.attempt { state.map(mutation) }
    }
    writeLock {
      state = newStateM.flatMap {
        case Right((a, _)) ⇒ M.pure(a)
        case Left(_)       ⇒ state
      }
    }
    newStateM.map(_.right.map(_._2))
  }

  def mutateM(mutationF: A ⇒ M[A]): M[Either[Throwable, Unit]] = writeLock {
    val newState: M[Either[Throwable, A]] = M.attempt { state.flatMap(mutationF) }
    state = newState.flatMap {
      case Right(a) ⇒ M.pure(a)
      case Left(_)  ⇒ state
    }

    newState.map(_.right.map(_ ⇒ {}))
  }

  def mutateFlatMapSync[B](mutationF: A ⇒ M[(A, B)]): M[Either[Throwable, B]] = writeLock {
    val newState: M[Either[Throwable, (A, B)]] = M.attempt { state.flatMap(mutationF) }
    state = newState.flatMap {
      case Right((a, _)) ⇒ M.pure(a)
      case Left(_)       ⇒ state
    }

    newState.map(_.right.map(_._2))
  }

  def mutateFlatMap[B](mutationF: A ⇒ M[(A, B)]): M[Either[Throwable, B]] = {
    val newStateM: M[Either[Throwable, (A, B)]] = readLock {
      M.attempt { state.flatMap(mutationF) }
    }
    writeLock {
      state = newStateM.flatMap {
        case Right((a, _)) ⇒ M.pure(a)
        case Left(_)       ⇒ state
      }
    }
    newStateM.map(_.right.map(_._2))
  }

  def lookUp: M[A] = readLock { state }
}

object StateEff {
  def apply[M[_], A](initialState: A)(implicit M: MonadError[M, Throwable]): StateEff[M, A] =
    new StateEff(initialState.pure[M])

  def lift[M[_], A](ma: M[A])(implicit M: MonadError[M, Throwable]): StateEff[M, A] = new StateEff(ma)
}