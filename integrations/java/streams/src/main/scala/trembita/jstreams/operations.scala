package trembita.jstreams

import java.util.{Comparator, Spliterator, Spliterators}
import java.util.function.{BiConsumer, BiFunction, BinaryOperator}

import trembita.operations._
import java.util.stream._

import scala.collection.JavaConverters._
import cats.{~>, Id}

import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag
import scala.compat.java8.FunctionConverters._

trait operations {
  implicit val canFoldJStream: CanReduce.Aux[Stream, Id] = new CanReduce[Stream] {
    type Result[X] = X

    def reduceOpt[A: ClassTag](fa: Stream[A])(
        f: (A, A) => A
    ): Option[A] = {
      fa.reduce(
        Option.empty[A],
        new BiFunction[Option[A], A, Option[A]] {
          def apply(t: Option[A], u: A): Option[A] = t match {
            case Some(acc) => Some(f(acc, u))
            case None      => Some(u)
          }
        },
        new BinaryOperator[Option[A]] {
          def apply(accOpt1: Option[A], accOpt2: Option[A]): Option[A] = (accOpt1, accOpt2) match {
            case (Some(acc1), Some(acc2)) => Some(f(acc1, acc2))
            case (_: Some[A], None)       => accOpt1
            case (None, _: Some[A])       => accOpt2
            case _                        => None
          }
        }
      )
    }

    def reduce[A: ClassTag](fa: Stream[A])(f: (A, A) => A): A =
      reduceOpt(fa)(f).get
  }

  implicit val jstreamHasSize: HasSize.Aux[Stream, Id] = new HasSize[Stream] {
    type Result[X] = X
    def size[A](fa: Stream[A]): Int = fa.count().toInt
  }

  implicit val canSortStream: CanSort[Stream] = new CanSort[Stream] {
    def sorted[A: Ordering: ClassTag](fa: Stream[A]): Stream[A] =
      sortedBy(fa)(identity)

    def sortedBy[A: ClassTag, B: Ordering: ClassTag](fa: Stream[A])(
        f: A => B
    ): Stream[A] = fa.sorted(Ordering.by(f))
  }

  implicit val canDropStream: CanDrop[Stream] = new CanDrop[Stream] { def drop[A](fa: Stream[A], n: Int): Stream[A] = fa.skip(n) }

  implicit val canDistinctByStream: CanDistinctBy[Stream] = new CanDistinctBy[Stream] {
    override def distinct[A: ClassTag](fa: Stream[A]): Stream[A] = fa.distinct()

    def distinctBy[A: ClassTag, B: ClassTag](fa: Stream[A])(
        f: A => B
    ): Stream[A] = {
      var seen = Set.empty[B]
      fa.filter(((a: A) => {
        val b = f(a)
        if (seen(b)) false
        else {
          seen += b
          true
        }
      }).asJava)
    }
  }

  implicit val canZipStream: CanZip[Stream] = new CanZip[Stream] {
    def zip[A: ClassTag, B: ClassTag](
        fa: Stream[A],
        fb: Stream[B]
    ): Stream[(A, B)] = {
      val isParallel    = fa.isParallel || fb.isParallel
      val faSpliterator = fa.spliterator()
      val fbSpliterator = fb.spliterator()
      val iterator      = (Spliterators.iterator(faSpliterator).asScala zip Spliterators.iterator(fbSpliterator).asScala).asJava
      StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, faSpliterator.characteristics() & fbSpliterator.characteristics()),
        isParallel
      )
    }
  }

  implicit def sequentialToJavaStreams[ST <: StreamType](implicit creator: StreamCreator[ST]): InjectTaggedK[Vector, Stream] =
    new InjectTaggedK[Vector, Stream] {
      def apply[A: ClassTag](fa: Vector[A]): Stream[A] = creator.apply(fa.asJava.stream())
    }

  implicit def parallelToJavaStreams[ST <: StreamType](implicit creator: StreamCreator[ST]): InjectTaggedK[ParVector, Stream] =
    new InjectTaggedK[ParVector, Stream] {
      def apply[A: ClassTag](
          fa: ParVector[A]
      ): Stream[A] = creator.apply(fa.seq.asJava.stream())
    }
}
