package trembita.outputs.internal

import cats.MonadError
import cats.evidence.Liskov
import shapeless.=:!=

import scala.language.higherKinds

trait lowPriorityTricks extends Serializable {
  implicit def chainTuples[A, B](implicit p0: A, p1: B): (A, B) = (p0, p1)
}
