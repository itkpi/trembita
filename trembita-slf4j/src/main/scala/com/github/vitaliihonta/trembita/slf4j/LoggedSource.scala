package com.github.vitaliihonta.trembita.slf4j

import org.slf4j.Logger
import com.github.vitaliihonta.trembita._
import com.github.vitaliihonta.trembita.parallel._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal


protected[trembita]
class LoggedSource[+A](logger: Logger, source: LazyList[A])
  extends BaseLazyList[A] {

  def map[B](f: A => B): LazyList[B] = new LoggedSource[B](logger, source.map(f))
  def flatMap[B](f: A => Iterable[B]): LazyList[B] = new LoggedSource[B](logger, source.flatMap(f))
  def filter(p: A => Boolean): LazyList[A] = new LoggedSource[A](logger, source.filter(p))
  def collect[B](pf: PartialFunction[A, B]): LazyList[B] = new LoggedSource[B](logger, source.collect(pf))
  def force: Iterable[A] = source.force
  def iterator: Iterator[A] = source.iterator
  override def par(implicit ec: ExecutionContext): ParLazyList[A] = new ParSource[A](this.force)
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: A => Future[B])(implicit ec: ExecutionContext): LazyList[B] = {
    new LoggedSource[B](logger, source.mapAsync(timeout, parallelism)(f))
  }

  override def log(toString: A => String = _.toString): LazyList[A] = this.map { a => logger.info(toString(a)); a }
  def info(toString: A => String = _.toString): LazyList[A] = log(toString)
  def debug(toString: A => String = _.toString): LazyList[A] =
    this.map { a => logger.debug(toString(a)); a }
  override def tryMap[B](f: A => Try[B]): LazyList[B] =
    this.flatMap { a =>
      val res = f(a)
      res.recoverWith {
        case NonFatal(e) =>
          logger.error("Failed tryMap", e)
          res
      }.toOption
    }
}

object LoggedSource {
  def apply[A](logger: Logger)(lazyList: LazyList[A]): LazyList[A] =
    new LoggedSource[A](logger, lazyList)
}
