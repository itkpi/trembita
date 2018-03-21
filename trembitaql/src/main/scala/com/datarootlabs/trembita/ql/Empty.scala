package com.datarootlabs.trembita.ql

trait Empty[A] extends Any {
  def empty: A
}

object Empty {
  def apply[A](implicit E: Empty[A]): Empty[A] = E
}