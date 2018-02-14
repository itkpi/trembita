package com.honta.trembita


import com.honta.trembita.internal.ListUtils
import org.scalatest.FlatSpec

class StrictTest extends FlatSpec {
  "LazyList operations" should "not be executed until 'force'" in {
    val list = LazyList(1, 2, 3)
    list.map(_ => throw new Exception("Bang"))
    assert(true)
  }

  "LazyList[Int].map(square)" should "be mapped squared" in {
    val list: LazyList[Int] = LazyList(1, 2, 3)
    val res: List[Int] = list.map(i => i * i).force.toList
    assert(res == List(1, 4, 9))
  }

  "LazyList[Int].filter(isEven)" should "contain only even numbers" in {
    val list: LazyList[Int] = LazyList(1, 2, 3)
    val res: List[Int] = list.filter(_ % 2 == 0).force.toList
    assert(res == List(2))
  }

  "LazyList[String].collect(toInt)" should "be LazyList[Int]" in {
    val list: LazyList[String] = LazyList("1", "2", "3", "abc")
    val res = list.collect {
      case str if str.forall(_.isDigit) => str.toInt
    }.force.toList
    assert(res == List(1, 2, 3))
  }

  "LazyList[String].flatMap(getWords)" should "be a list of words" in {
    val list: LazyList[String] = LazyList("Hello world", "hello you to")
    val res = list.flatMap(_.split("\\s")).force.toList
    assert(res == List("Hello", "world", "hello", "you", "to"))
  }

  "LazyList[Int].sorted" should "be sorted" in {
    val list: LazyList[Int] = LazyList(5, 4, 3, 1)
    val res = list.sorted.force.toList
    assert(res == List(1, 3, 4, 5))
  }

  "LazyList[String]" should "be sorted by length" in {
    val list = LazyList("a", "abcd", "bcd")
    val res = list.sortBy(_.length).force.toList
    assert(res == List("a", "bcd", "abcd"))
  }

  "LazyList[Int].reduce(_+_)" should "produce list sum" in {
    val list = LazyList(1, 2, 3)
    val res = list.reduce(_ + _)
    assert(res == 6)
  }

  "LazyList[Int].reduce" should "throw NoSuchElementException on empty list" in {
    val list = LazyList.empty[Int]
    assertThrows[NoSuchElementException](list.reduce(_ + _))
  }

  "LazyList[Int].reduceOpt" should "work" in {
    val list = LazyList(1, 2, 3)
    val res = list.reduceOpt(_ + _)
    assert(res.contains(6))
  }

  "LazyList[Int].reduceOpt" should "produce None on empty list" in {
    val list = LazyList.empty[Int]
    val res = list.reduceOpt(_ + _)
    assert(res.isEmpty)
  }

  "LazyList.size" should "return list size" in {
    val list = LazyList(1, 2, 3)
    assert(list.size == 3)
  }

  "LazyList.groupBy" should "group elements" in {
    val list = LazyList(1, 2, 3, 4)
    val grouped = list.groupBy(_ % 2 == 0)
      .mapValues(_.toList)
      .sortBy(_._1)
      .force.toList

    assert(grouped == List(false -> List(1, 3), true -> List(2, 4)))
  }

  "LazyList.distinct" should "work" in {
    val list = LazyList(1, 2, 3, 1, 3, 2, 1)
    val distinct = list.distinct.force.toList.sorted
    assert(distinct == List(1, 2, 3))
  }

  "LazyList operations" should "be executed on each force" in {
    var x: Int = 0
    val list = LazyList(1, 2, 3).map { i => x += 1; i }
    val res1 = list.force
    assert(x == 3)
    val res2 = list.force
    assert(x == 6)
  }

  "LazyList operations after .cache()" should "be executed exactly once" in {
    var x: Int = 0
    val list = LazyList(1, 2, 3).map { i => x += 1; i }.cache()
    val res1 = list.force
    assert(x == 3)
    val res2 = list.force
    assert(x == 3)
  }

  "LazyList.find" should "successfully find an element" in {
    val list = LazyList(1, 2, 3)
    assert(list.find(_ % 2 == 0) contains 2)
  }

  "LazyList.contains" should "work correctly" in {
    val list = LazyList(1, 2, 3)
    assert(list.contains(2))
    assert(!list.contains(5))
  }

  "LazyList.forall" should "work correctly" in {
    val list = LazyList(1, 2, 3)
    assert(list.forall(_ > 0))
    assert(!list.forall(_ % 2 == 0))
  }

  "split(n)" should "produce collection with n subcollections" in {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    val grouped = ListUtils.split(4)(list).map(_.toList).toList
    assert(grouped == List(List(1, 2), List(3, 4), List(5, 6), List(7, 8)))
    val list2 = List(1, 2)
    val grouped2 = ListUtils.split(4)(list2).map(_.toList).toList
    assert(grouped2 == List(List(1, 2)))
    val list3 = Nil
    val grouped3 = ListUtils.split(2)(list3).map(_.toList).toList
    assert(grouped3 == Nil)
  }
}
