package trembita.jstreams

import trembita._
import java.util.stream._

import cats.{Id, Monad, Traverse}
import cats.instances.list._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import java.{util => ju}
import scala.compat.java8.FunctionConverters._
import scala.language.higherKinds

sealed trait StreamType
object StreamType {
  sealed trait Sequential extends StreamType
  sealed trait Parallel   extends StreamType
}

trait StreamCreator[T <: StreamType] {
  def create[A](list: ju.List[A]): Stream[A]
  def create[A](spliterator: ju.Spliterator[A]): Stream[A]
  def apply[A](stream: Stream[A]): Stream[A]
}
object StreamCreator {
  implicit val seqCreator: StreamCreator[StreamType.Sequential] = new StreamCreator[StreamType.Sequential] {
    def create[A](list: ju.List[A]): Stream[A]               = list.stream()
    def create[A](spliterator: ju.Spliterator[A]): Stream[A] = StreamSupport.stream(spliterator, false)
    def apply[A](stream: Stream[A]): Stream[A]               = stream.sequential()
  }

  implicit val parCreator: StreamCreator[StreamType.Parallel] = new StreamCreator[StreamType.Parallel] {
    def create[A](list: ju.List[A]): Stream[A]               = list.parallelStream()
    def create[A](spliterator: ju.Spliterator[A]): Stream[A] = StreamSupport.stream(spliterator, true)
    def apply[A](stream: Stream[A]): Stream[A]               = stream.parallel()
  }

  def create[T <: StreamType, A](list: ju.List[A])(implicit creator: StreamCreator[T]): Stream[A]               = creator create list
  def create[T <: StreamType, A](spliterator: ju.Spliterator[A])(implicit creator: StreamCreator[T]): Stream[A] = creator create spliterator
  def apply[T <: StreamType, A](stream: Stream[A])(implicit creator: StreamCreator[T]): Stream[A]               = creator apply stream
}

abstract class JavaStreams[T <: StreamType: StreamCreator] extends Environment {
  type Repr[X] = Stream[X]

  type Run[G[_]] = Monad[G]
  type Result[X] = X

  val FlatMapResult: Monad[Result] = Monad[Id]
  val FlatMapRepr: ApplicativeFlatMap[Stream] = new ApplicativeFlatMap[Stream] {

    def map[A, B: ClassTag](fa: Stream[A])(f: A => B): Stream[B] = fa.map(f.asJava)

    def mapConcat[A, B: ClassTag](fa: Stream[A])(f: A => Iterable[B]): Stream[B] =
      fa.flatMap(((a: A) => StreamCreator.create(f(a).asJava.spliterator())).asJava)
  }
  val TraverseRepr: TraverseTag[Stream, Run] = new TraverseTag[Stream, Run] {
    def traverse[G[_], A, B: ClassTag](fa: Stream[A])(f: A => G[B])(
        implicit G: Monad[G]
    ): G[Stream[B]] = {
      val gb = Traverse[List].traverse[G, A, B](fa.collect(Collectors.toList[A]).asScala.toList)(f)
      G.map(gb)(_.asJava.stream())
    }
  }
  def foreach[A](repr: Stream[A])(f: A => Unit): Unit = repr.forEach(f.asJava)
  def collect[A, B: ClassTag](repr: Stream[A])(pf: PartialFunction[A, B]): Stream[B] =
    repr.flatMap(
      (
          (a: A) =>
            StreamCreator.create {
              if (pf.isDefinedAt(a)) ju.Arrays.asList(pf(a))
              else ju.Arrays.asList[B]()
            }
      ).asJava
    )
  def distinctKeys[A: ClassTag, B: ClassTag](repr: Stream[(A, B)]): Stream[(A, B)] =
    canDistinctByStream.distinctBy(repr)(_._2)
  def concat[A](xs: Stream[A], ys: Stream[A]): Stream[A] = Stream.concat(xs, ys)
  def memoize[A: ClassTag](xs: Stream[A]): Stream[A]     = StreamCreator.create(xs.collect(Collectors.toList[A]))
}

object JavaStreams {
  implicit def instance[T <: StreamType: StreamCreator]: JavaStreams[T] = new JavaStreams[T] {}
}
