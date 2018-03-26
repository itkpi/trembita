package com.datarootlabs.trembita

import cats.data.Kleisli

import scala.language.higherKinds
import internal._
import parallel._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Random, Try}
import cats.effect._
import cats.implicits._


trait DataPipeline[+A] {
  def map[B](f: A => B): DataPipeline[B]
  def flatMap[B](f: A => Iterable[B]): DataPipeline[B]
  def filter(p: A => Boolean): DataPipeline[A]
  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B]
  def transform[B >: A, C](flow: Kleisli[DataPipeline, DataPipeline[B], C]): DataPipeline[C] = flow.run(this)

  def force: Iterable[A]
  protected[trembita] def runM[B >: A, M[_]](implicit M: Sync[M]): M[Iterable[B]] = M.delay(force)

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

  def sorted[B >: A : Ordering : ClassTag]: DataPipeline[B]
  def sortBy[B >: A, BB: Ordering](f: A => BB)(implicit ctg: ClassTag[B]): DataPipeline[B]
  def distinct: DataPipeline[A] = distinctBy(identity)
  def distinctBy[B](f: A => B): DataPipeline[A] = this.groupBy(f).map { case (_, group) => group.head }

  def par(implicit ec: ExecutionContext): ParDataPipeline[A]
  def seq: DataPipeline[A]
  def groupBy[K](f: A => K): DataPipeline[(K, Iterable[A])] = new GroupByList[K, A](f, this)

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
  def cache(): DataPipeline[A] = new CachedSource(this.force)

  def slide[B, C](init: C)(add: (C, A) => C, extract: PartialFunction[C, (C, B)]): DataPipeline[B] = new StrictSource[B]({
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

  def log[B >: A](toString: B => String = (b: B) => b.toString): DataPipeline[A] = this.map { a => println(toString(a)); a }
  def tryMap[B](f: A => Try[B]): DataPipeline[B] = this.flatMap(a => f(a).toOption)

  def mapAsync[B](timeout: FiniteDuration,
                  parallelism: Int = ParDataPipeline.defaultParallelism)
                 (f: A => Future[B])
                 (implicit ec: ExecutionContext): DataPipeline[B]

  def :+[B >: A](elem: B): DataPipeline[B]
  def ++[B >: A](that: DataPipeline[B]): DataPipeline[B]
}

protected[trembita] trait BaseDataPipeline[+A] extends DataPipeline[A] {
  override def seq: DataPipeline[A] = this
  override def sortBy[B >: A, BB: Ordering](f: A => BB)(implicit ctg: ClassTag[B]): DataPipeline[B] =
    new SortedSource[B](this)(new Ordering[B] {
      override def compare(x: B, y: B): Int = {
        val xb: BB = f(x.asInstanceOf[A])
        val yb: BB = f(y.asInstanceOf[A])
        Ordering[BB].compare(xb, yb)
      }
    }, ctg)
  override def sorted[B >: A : Ordering : ClassTag]: DataPipeline[B] = new SortedSource[B](this)
  override def take(n: Int): Iterable[A] = this.force.take(n)
  override def drop(n: Int): Iterable[A] = this.force.drop(n)
  override def headOption: Option[A] = this.force.headOption
  override def find[B >: A](p: B => Boolean): Option[B] = this.force.find(p)
}

object DataPipeline {
  def apply[A](xs: A*): DataPipeline[A] = new StrictSource[A](xs)
  def from[A](it: => Iterable[A]): DataPipeline[A] = new StrictSource[A](it)
  def empty[A]: DataPipeline[A] = new StrictSource[A](Nil)
  def repeat[A](times: Int)(fa: => A): DataPipeline[A] = new StrictSource[A]((1 to times).map(_ => fa))
  def randomInts(size: Int): DataPipeline[Int] = repeat(size)(Random.nextInt())
  def fromFile(fileName: String): DataPipeline[String] = new StrictSource[String](
    scala.io.Source.fromFile(fileName).getLines().toIterable
  )
}