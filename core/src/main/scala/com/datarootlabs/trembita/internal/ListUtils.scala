package com.datarootlabs.trembita.internal

object ListUtils {
  def split[A](parts: Int)(iterable: Iterable[A]): Iterable[Iterable[A]] = {
    iterable.size match {
      case 0 => Nil
      case size if size < parts => List(iterable)
      case size                 =>
        val groupSize = size / parts
        iterable.grouped(groupSize).toIterable
    }
  }
}
