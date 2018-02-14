package com.honta.trembita.distributed.internal


import akka.actor._
import akka.pattern.ask
import com.honta.trembita.distributed.DistributedList
import DistributedList.defaultTimeout
import Communication._
import scala.concurrent.{Await, ExecutionContext, Future}


class DistributedMapList[A, B](@transient val createActor: () => ActorRef)
                              (val f: A => B)
                              (@transient implicit val ec: ExecutionContext) extends DistributedList[B] {

  override def map[C](f2: B => C): DistributedList[C] =
    new DistributedMapList[A, C](createActor)(f2.compose(f))

  override def flatMap[C](f2: B => Iterable[C]): DistributedList[C] =
    new DistributedFlatMapList[A, C](createActor)(f2.compose(f))

  override def filter(p: B => Boolean): DistributedList[B] =
    new DistributedFlatMapList[A, B](createActor)(a => Some(f(a)).filter(p))

  override def collect[C](pf: PartialFunction[B, C]): DistributedList[C] =
    new DistributedFlatMapList[A, C](createActor)(a => Some(f(a)).collect(pf))

  override def force: Iterable[B] = {
    val taskActor: ActorRef = createActor()
    val futureResult: Future[Iterable[B]] = (taskActor ? TransformSource[A, B](_.map(f)))
      .flatMap {
        case Failed(cause)                => Future.failed(cause)
        case TransformationResult(result) => Future.successful(result.asInstanceOf[Iterable[B]])
      }

    Await.result(futureResult, defaultTimeout.duration)
  }
}

class DistributedFlatMapList[A, B](@transient val createActor: () => ActorRef)
                                  (val f: A => Iterable[B])
                                  (@transient implicit val ec: ExecutionContext) extends DistributedList[B] {

  private def newFlatMapList[C](f2: A => Iterable[C]): DistributedList[C] =
    new DistributedFlatMapList[A, C](createActor)(f2)

  override def map[C](f2: B => C): DistributedList[C] = newFlatMapList(f(_).map(f2))
  override def flatMap[C](f2: B => Iterable[C]): DistributedList[C] = newFlatMapList(f(_).flatMap(f2))
  override def filter(p: B => Boolean): DistributedList[B] = newFlatMapList(f(_).filter(p))
  override def collect[C](pf: PartialFunction[B, C]): DistributedList[C] = newFlatMapList(f(_).collect(pf))

  override def force: Iterable[B] = {
    val taskActor: ActorRef = createActor()
    val futureResult: Future[Iterable[B]] = (taskActor ? TransformSource[A, B](_.flatMap(f)))
      .flatMap {
        case Failed(cause)                => Future.failed(cause)
        case TransformationResult(result) => Future.successful(result.asInstanceOf[Iterable[B]])
      }
    Await.result(futureResult, defaultTimeout.duration)
  }
}

class DistributedCollectList[A, B](@transient val createActor: () => ActorRef)
                                  (val pf: PartialFunction[A, B])
                                  (@transient implicit val ec: ExecutionContext) extends DistributedList[B] {
  private def newCollectList[C](pf2: PartialFunction[A, C]): DistributedList[C] =
    new DistributedCollectList[A, C](createActor)(pf2)

  override def map[C](f2: B => C): DistributedList[C] = newCollectList(pf.andThen(f2))
  override def flatMap[C](f2: B => Iterable[C]): DistributedList[C] =
    new DistributedFlatMapList[A, C](createActor)({ a =>
      if (pf.isDefinedAt(a)) f2(pf(a))
      else Iterable.empty[C]
    })

  override def filter(p: B => Boolean): DistributedList[B] = newCollectList(pf.andThen({
    case b if p(b) => b
  }))

  override def collect[C](pf2: PartialFunction[B, C]): DistributedList[C] = newCollectList(pf.andThen(pf2))

  override def force: Iterable[B] = {
    val taskActor: ActorRef = createActor()
    val futureResult: Future[Iterable[B]] = (taskActor ? TransformSource[A, B](_.collect(pf)))
      .flatMap {
        case Failed(cause)                => Future.failed(cause)
        case TransformationResult(result) => Future.successful(result.asInstanceOf[Iterable[B]])
      }
    Await.result(futureResult, defaultTimeout.duration)
  }
}


class DistributedSource[A](@transient system: ActorSystem,
                           @transient master: ActorRef,
                           @transient val source: () => Iterator[A],
                           val parallelism: Int)
                          (@transient implicit val ec: ExecutionContext) extends DistributedList[A] {


  val createActor: () => ActorRef = () =>
    system.actorOf(Props(new CoordinatorActor[A](master, parallelism, source)))

  override def map[B](f: A => B): DistributedList[B] = new DistributedMapList[A, B](createActor)(f)
  override def flatMap[B](f: A => Iterable[B]): DistributedList[B] = new DistributedFlatMapList[A, B](createActor)(f)
  override def filter(p: A => Boolean): DistributedList[A] =
    new DistributedCollectList[A, A](createActor)({ case a if p(a) => a })

  override def collect[B](pf: PartialFunction[A, B]): DistributedList[B] = new DistributedCollectList[A, B](createActor)(pf)

  override def force: Iterable[A] = source().toVector
  override def iterator: Iterator[A] = source()
}