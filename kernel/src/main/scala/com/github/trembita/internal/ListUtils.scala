package com.github.trembita.internal

import scala.collection.generic.CanBuildFrom


/**
  * Used in [[com.github.trembita]]
  **/
object ListUtils {
  /**
    * Having some {{{ Iterable[A] = 1 to 10 }}}
    * and parts = 3
    * produces {{{ Iterable[Iterable[A]] =  List( List(1, 2, 3), List(4, 5, 6), List(7, 8, 9, 10) ) }}}
    *
    * @tparam A - Iterable's elements data type
    * @param parts    - a number of Iterables that result will contain
    * @param iterable - collection to split
    * @return - iterable divided on {{{ parts }}} parts
    **/
  def batch[A](parts: Int)(iterable: Iterable[A]): Iterable[Iterable[A]] = {
    iterable.size match {
      case 0                    => Nil
      case size if size < parts => List(iterable)
      case size                 =>
        val groupSize = size / parts
        iterable.grouped(groupSize).toIterable
    }
  }
}
