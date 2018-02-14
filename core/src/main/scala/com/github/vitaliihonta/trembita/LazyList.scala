package com.github.vitaliihonta.trembita

import internal._
import parallel._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Random, Try}


trait LazyList[+A] {
  def map[B](f: A => B): LazyList[B]
  def flatMap[B](f: A => Iterable[B]): LazyList[B]
  def filter(p: A => Boolean): LazyList[A]

  def collect[B](pf: PartialFunction[A, B]): LazyList[B]
  def force: Iterable[A]

  def reduce[B >: A](f: (B, B) => B): B = reduceOpt(f).get
  def reduceOpt[B >: A](f: (B, B) => B): Option[B] = foldLeft(Option.empty[B]) {
    case (None, b) => Some(b)
    case (a, b)    => a.map(f(_, b))
  }
  def foldLeft[C](zero: C)(f: (C, A) => C): C = {
    val forced: Iterable[A] = this.force
    forced.foldLeft(zero)(f)
  }

  def size: Int = foldLeft(0)((s, _) => s + 1)

  def sorted[B >: A : Ordering : ClassTag]: LazyList[B]
  def sortBy[B >: A, BB: Ordering](f: A => BB)(implicit ctg: ClassTag[B]): LazyList[B]
  def distinct: LazyList[A] = distinctBy(identity)
  def distinctBy[B](f: A => B): LazyList[A] = this.groupBy(f).map { case (_, group) => group.head }

  def par(implicit ec: ExecutionContext): ParLazyList[A]
  def seq: LazyList[A]
  def groupBy[K](f: A => K): LazyList[(K, Iterable[A])] = new GroupByList[K, A](f, this)

  def take(n: Int): Iterable[A]
  def drop(n: Int): Iterable[A]

  def find[B >: A](p: B => Boolean): Option[B]
  def exists(p: A => Boolean): Boolean = find(p).nonEmpty
  def contains[B >: A](elem: B): Boolean = find(_ == elem).nonEmpty
  def forall(p: A => Boolean): Boolean = find(!p(_)).isEmpty

  def head: A = headOption.get
  def headOption: Option[A]
  def foreach(f: A => Unit): Unit = this.force.foreach(f)

  def iterator: Iterator[A]
  def cache(): LazyList[A] = new CachedSource(this.force)

  def slide[B, C](init: C)(add: (C, A) => C, extract: PartialFunction[C, (C, B)]): LazyList[B] = new StrictSource[B]({
    val forced: Iterable[A] = this.force
    val builder = Vector.newBuilder[B]
    var acc: C = init
    for (item <- forced) {
      if (!extract.isDefinedAt(acc)) {
        acc = add(acc, item)
      } else {
        val (newAcc, elem) = extract(acc)
        builder += elem
        acc = add(newAcc, item)
      }
    }
    builder.result()
  })

  def log(toString: A => String = _.toString): LazyList[A] = this.map { a => println(toString(a)); a }
  def tryMap[B](f: A => Try[B]): LazyList[B] = this.flatMap(a => f(a).toOption)

  def mapAsync[B](timeout: FiniteDuration,
                  parallelism: Int = ParLazyList.defaultParallelism)
                 (f: A => Future[B])
                 (implicit ec: ExecutionContext): LazyList[B]
}

protected[trembita] trait BaseLazyList[+A] extends LazyList[A] {
  override def seq: LazyList[A] = this
  override def sortBy[B >: A, BB: Ordering](f: A => BB)(implicit ctg: ClassTag[B]): LazyList[B] =
    new SortedSource[B](this)(new Ordering[B] {
      override def compare(x: B, y: B): Int = {
        val xb: BB = f(x.asInstanceOf[A])
        val yb: BB = f(y.asInstanceOf[A])
        Ordering[BB].compare(xb, yb)
      }
    }, ctg)
  override def sorted[B >: A : Ordering : ClassTag]: LazyList[B] = new SortedSource[B](this)
  override def take(n: Int): Iterable[A] = this.force.take(n)
  override def drop(n: Int): Iterable[A] = this.force.drop(n)
  override def headOption: Option[A] = this.force.headOption
  override def find[B >: A](p: B => Boolean): Option[B] = this.force.find(p)
}

object LazyList {
  def apply[A](xs: A*): LazyList[A] = new StrictSource[A](xs)
  def from[A](it: => Iterable[A]): LazyList[A] = new StrictSource[A](it)
  def empty[A]: LazyList[A] = new StrictSource[A](Nil)
  def repeat[A](times: Int)(fa: => A): LazyList[A] = new StrictSource[A]((1 to times).map(_ => fa))
  def randomInts(size: Int): LazyList[Int] = repeat(size)(Random.nextInt())
  def fromFile(fileName: String): LazyList[String] = new IteratorSource[String](scala.io.Source.fromFile(fileName).getLines())
}