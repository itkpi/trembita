package trembita

import cats.kernel.Monoid
import trembita.outputs.internal._
import scala.language.higherKinds

object Output {
  @inline def collection[Col[x] <: Iterable[x]]: collectionDsl[Col]             = new collectionDsl[Col]
  @inline def foreach[A](f: A => Unit): foreachDsl[A]                           = new foreachDsl[A](f)
  @inline def onComplete[Er, A](f: Either[Er, A] => Unit): onCompleteDsl[Er, A] = new onCompleteDsl[Er, A](f)
  @inline def reduce[Er, A](f: (A, A) => A): reduceDsl[Er, A]                   = new reduceDsl[Er, A](f)
  @inline def combineAll[Er, A](implicit monoid: Monoid[A])                     = new foldDslWithErrors[Er, A](monoid.empty -> monoid.combine)
  @inline def reduceOpt[Er, A](f: (A, A) => A): reduceOptDsl[Er, A]             = new reduceOptDsl[Er, A](f)
  @inline def foldLeft[A, B](zero: B)(f: (B, A) => B): foldLeftDsl[A, B]        = new foldLeftDsl[A, B](zero -> f)
  @inline def ignore[A]: onCompleteDsl[Nothing, A]                              = ignoreInstance.asInstanceOf[onCompleteDsl[Nothing, A]]

  val fold: foldDsl                 = new foldDsl()
  val foldF: foldFDsl               = new foldFDsl()
  val set: collectionDsl[Set]       = collection[Set]
  val vector: collectionDsl[Vector] = collection[Vector]
  val size: sizeDsl                 = new sizeDsl()

  private val ignoreInstance: onCompleteDsl[Nothing, Any] = onComplete[Nothing, Any](_ => {})
}
