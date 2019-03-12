package trembita.inputs

import java.io.IOException
import java.net.{URI, URL}
import java.nio.file.Path
import cats.{Monad, MonadError}
import trembita._
import trembita.internal.StrictSource
import scala.io.Source
import scala.language.higherKinds
import scala.reflect.ClassTag

class FileInput[F[_]] private[trembita] () extends InputT[F, IOException, Sequential, FileInput.Props] {
  def create[A: ClassTag](props: FileInput.Props[A])(
      implicit F: MonadError[F, IOException]
  ): BiDataPipelineT[F, IOException, A, Sequential] =
    new StrictSource[F, IOException, A](F.pure(Source.fromURL(props.url).getLines().map(props.gen)), F)
}

object FileInput {
  class Props[A] private[trembita] (private[trembita] val url: URL, private[trembita] val gen: String => A)

  @inline def props(path: Path): Props[String]                 = props[String](path)(identity)
  @inline def props(uri: URI): Props[String]                   = props[String](uri.toURL)(identity)
  @inline def props(url: URL): Props[String]                   = props[String](url)(identity)
  @inline def props[A](path: Path)(gen: String => A): Props[A] = props[A](path.toUri.toURL)(gen)
  @inline def props[A](uri: URI)(gen: String => A): Props[A]   = props[A](uri.toURL)(gen)
  @inline def props[A](url: URL)(gen: String => A): Props[A]   = new Props[A](url, gen)
}

class FileInputF[F[_]](implicit ctgF: ClassTag[F[_]]) extends InputT[F, IOException, Sequential, FileInputF.Props[F, ?]] {
  private implicit def factg[A: ClassTag]: ClassTag[F[A]] =
    ClassTag[F[A]](ctgF.runtimeClass)

  def create[A: ClassTag](props: FileInputF.Props[F, A])(
      implicit F: MonadError[F, IOException]
  ): BiDataPipelineT[F, IOException, A, Sequential] =
    new StrictSource[F, IOException, F[A]](F.pure(Source.fromURL(props.url).getLines().map(props.gen)), F)
      .mapMImpl[F[A], A](fa => fa)
}

object FileInputF {
  class Props[F[_], A] private[trembita] (private[trembita] val url: URL, private[trembita] val gen: String => F[A])

  @inline def props[F[_], A](path: Path)(gen: String => F[A]): Props[F, A] = props[F, A](path.toUri.toURL)(gen)
  @inline def props[F[_], A](uri: URI)(gen: String => F[A]): Props[F, A]   = props[F, A](uri.toURL)(gen)
  @inline def props[F[_], A](url: URL)(gen: String => F[A]): Props[F, A]   = new Props[F, A](url, gen)
}
