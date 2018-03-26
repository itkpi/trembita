package com.datarootlabs.trembita.distributed.internal


import akka.actor._
import akka.pattern.ask
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._
import com.datarootlabs.trembita.distributed._
import DistributedDataPipeline.defaultTimeout
import Communication._
import scala.concurrent.{Await, ExecutionContext, Future}


class DistributedMapDataPipeline[A, B](@transient val createActor: () => ActorRef)
                                      (val f: A => B)
                                      (@transient implicit val ec: ExecutionContext) extends DistributedDataPipeline[B] {

  override def map[C](f2: B => C): DistributedDataPipeline[C] =
    new DistributedMapDataPipeline[A, C](createActor)(f2.compose(f))

  override def flatMap[C](f2: B => Iterable[C]): DistributedDataPipeline[C] =
    new DistributedFlatMapDataPipeline[A, C](createActor)(f2.compose(f))

  override def filter(p: B => Boolean): DistributedDataPipeline[B] =
    new DistributedFlatMapDataPipeline[A, B](createActor)(a => Some(f(a)).filter(p))

  override def collect[C](pf: PartialFunction[B, C]): DistributedDataPipeline[C] =
    new DistributedFlatMapDataPipeline[A, C](createActor)(a => Some(f(a)).collect(pf))

  override def force: Iterable[B] = {
    val taskActor: ActorRef = createActor()
    val futureResult: Future[Iterable[B]] = (taskActor ? TransformSource[A, B](_.map(f)))
      .flatMap {
        case Failed(cause)                => Future.failed(cause)
        case TransformationResult(result) => Future.successful(result.asInstanceOf[Iterable[B]])
      }

    Await.result(futureResult, defaultTimeout.duration)
  }

  override def :+[BB >: B](elem: BB): DataPipeline[BB] = ???
  override def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = ???
}

class DistributedFlatMapDataPipeline[A, B](@transient val createActor: () => ActorRef)
                                          (val f: A => Iterable[B])
                                          (@transient implicit val ec: ExecutionContext) extends DistributedDataPipeline[B] {

  private def newFlatMapList[C](f2: A => Iterable[C]): DistributedDataPipeline[C] =
    new DistributedFlatMapDataPipeline[A, C](createActor)(f2)

  override def map[C](f2: B => C): DistributedDataPipeline[C] = newFlatMapList(f(_).map(f2))
  override def flatMap[C](f2: B => Iterable[C]): DistributedDataPipeline[C] = newFlatMapList(f(_).flatMap(f2))
  override def filter(p: B => Boolean): DistributedDataPipeline[B] = newFlatMapList(f(_).filter(p))
  override def collect[C](pf: PartialFunction[B, C]): DistributedDataPipeline[C] = newFlatMapList(f(_).collect(pf))

  override def force: Iterable[B] = {
    val taskActor: ActorRef = createActor()
    val futureResult: Future[Iterable[B]] = (taskActor ? TransformSource[A, B](_.flatMap(f)))
      .flatMap {
        case Failed(cause)                => Future.failed(cause)
        case TransformationResult(result) => Future.successful(result.asInstanceOf[Iterable[B]])
      }
    Await.result(futureResult, defaultTimeout.duration)
  }

  override def :+[BB >: B](elem: BB): DataPipeline[BB] = ???
  override def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = ???
}

class DistributedCollectDataPipeline[A, B](@transient val createActor: () => ActorRef)
                                          (val pf: PartialFunction[A, B])
                                          (@transient implicit val ec: ExecutionContext) extends DistributedDataPipeline[B] {
  private def newCollectList[C](pf2: PartialFunction[A, C]): DistributedDataPipeline[C] =
    new DistributedCollectDataPipeline[A, C](createActor)(pf2)

  override def map[C](f2: B => C): DistributedDataPipeline[C] = newCollectList(pf.andThen(f2))
  override def flatMap[C](f2: B => Iterable[C]): DistributedDataPipeline[C] =
    new DistributedFlatMapDataPipeline[A, C](createActor)({ a =>
      if (pf.isDefinedAt(a)) f2(pf(a))
      else Iterable.empty[C]
    })

  override def filter(p: B => Boolean): DistributedDataPipeline[B] = newCollectList(pf.andThen({
    case b if p(b) => b
  }))

  override def collect[C](pf2: PartialFunction[B, C]): DistributedDataPipeline[C] = newCollectList(pf.andThen(pf2))

  override def force: Iterable[B] = {
    val taskActor: ActorRef = createActor()
    val futureResult: Future[Iterable[B]] = (taskActor ? TransformSource[A, B](_.collect(pf)))
      .flatMap {
        case Failed(cause)                => Future.failed(cause)
        case TransformationResult(result) => Future.successful(result.asInstanceOf[Iterable[B]])
      }
    Await.result(futureResult, defaultTimeout.duration)
  }

  override def :+[BB >: B](elem: BB): DataPipeline[BB] = ???
  override def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = ???
}


class DistributedSource[A](@transient system: ActorSystem,
                           @transient master: ActorRef,
                           @transient val source: () => Iterator[A],
                           val parallelism: Int)
                          (@transient implicit val ec: ExecutionContext) extends DistributedDataPipeline[A] {


  val createActor: () => ActorRef = () =>
    system.actorOf(Props(new CoordinatorActor[A](master, parallelism, source)))

  override def map[B](f: A => B): DistributedDataPipeline[B] = new DistributedMapDataPipeline[A, B](createActor)(f)
  override def flatMap[B](f: A => Iterable[B]): DistributedDataPipeline[B] = new DistributedFlatMapDataPipeline[A, B](createActor)(f)
  override def filter(p: A => Boolean): DistributedDataPipeline[A] =
    new DistributedCollectDataPipeline[A, A](createActor)({ case a if p(a) => a })

  override def collect[B](pf: PartialFunction[A, B]): DistributedDataPipeline[B] = new DistributedCollectDataPipeline[A, B](createActor)(pf)

  override def force: Iterable[A] = source().toVector
  override def iterator: Iterator[A] = source()

  override def :+[BB >: A](elem: BB): DataPipeline[BB] =
    new DistributedSource(system, master, () ⇒ source() ++ Some(elem), parallelism)
  override def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] =
    new DistributedSource(system, master, () ⇒ source() ++ that.iterator, parallelism)
}