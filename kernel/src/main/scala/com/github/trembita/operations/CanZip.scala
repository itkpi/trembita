package com.github.trembita.operations

import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CanZip[F[_]] {
  def zip[A: ClassTag, B: ClassTag](fa: F[A], fb: F[B]): F[(A, B)]
}

object CanZip {
  implicit val canZipVectors: CanZip[Vector] = new CanZip[Vector] {
    def zip[A: ClassTag, B: ClassTag](fa: Vector[A], fb: Vector[B]): Vector[(A, B)] = fa zip fb
  }

  implicit val canZipParVectors: CanZip[ParVector] = new CanZip[ParVector] {
    def zip[A: ClassTag, B: ClassTag](fa: ParVector[A], fb: ParVector[B]): ParVector[(A, B)] = fa zip fb
  }
}
