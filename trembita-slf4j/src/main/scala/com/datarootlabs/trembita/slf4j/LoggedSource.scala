package com.datarootlabs.trembita.slf4j

import org.slf4j.Logger
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal


protected[trembita]
class LoggedSource[+A](logger: Logger, source: DataPipeline[A])
  extends BaseDataPipeline[A] {

  def map[B](f: A => B): DataPipeline[B] = new LoggedSource[B](logger, source.map(f))
  def flatMap[B](f: A => Iterable[B]): DataPipeline[B] = new LoggedSource[B](logger, source.flatMap(f))
  def filter(p: A => Boolean): DataPipeline[A] = new LoggedSource[A](logger, source.filter(p))
  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B] = new LoggedSource[B](logger, source.collect(pf))
  def force: Iterable[A] = source.force
  def iterator: Iterator[A] = source.iterator
  override def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSource[A](this.force)
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: A => Future[B])(implicit ec: ExecutionContext): DataPipeline[B] = {
    new LoggedSource[B](logger, source.mapAsync(timeout, parallelism)(f))
  }

  override def log[B >: A](toString: B => String = (b: B) => b.toString): DataPipeline[A] = this.map { a => logger.info(toString(a)); a }
  def info[B >: A](toString: A => String = (b: B) => b.toString): DataPipeline[A] = log(toString)
  def debug[B >: A](toString: A => String = (b: B) => b.toString): DataPipeline[A] =
    this.map { a => logger.debug(toString(a)); a }
  override def tryMap[B](f: A => Try[B]): DataPipeline[B] =
    this.flatMap { a =>
      val res = f(a)
      res.recoverWith {
        case NonFatal(e) =>
          logger.error("Failed tryMap", e)
          res
      }.toOption
    }

  override def :+[BB >: A](elem: BB): DataPipeline[BB] = new LoggedSource(logger, source :+ elem)
  override def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] = new LoggedSource(logger, source ++ that)
}

object LoggedSource {
  def apply[A](logger: Logger)(lazyList: DataPipeline[A]): DataPipeline[A] =
    new LoggedSource[A](logger, lazyList)
}
