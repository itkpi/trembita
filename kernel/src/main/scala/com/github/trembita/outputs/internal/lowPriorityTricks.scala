package com.github.trembita.outputs.internal

import scala.language.higherKinds

trait lowPriorityTricks {
  implicit def chainTuples[A, B](implicit p0: A, p1: B): (A, B) = (p0, p1)
}
