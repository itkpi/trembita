package com.github.trembita.inputs

import java.net.{URI, URL}
import java.nio.file.Path
import cats.{Id, Monad}
import com.github.trembita._
import com.github.trembita.internal.StrictSource
import scala.io.Source
import scala.reflect.ClassTag
import scala.language.higherKinds

class FileInput private[trembita] () extends InputT[Id, Sequential, FileInput.Props] {
  def create[A: ClassTag](props: FileInput.Props[A])(
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Sequential] = new StrictSource[Id, A](Source.fromURL(props.url).getLines().map(props.gen), F)
}

object FileInput {
  class PropsT[F[_], A] private[trembita] (private[trembita] val url: URL, private[trembita] val gen: String => F[A])
  type Props[A] = PropsT[Id, A]

  @inline def props(path: Path): Props[String]                 = props[String](path)(identity)
  @inline def props(uri: URI): Props[String]                   = props[String](uri.toURL)(identity)
  @inline def props(url: URL): Props[String]                   = props[String](url)(identity)
  @inline def props[A](path: Path)(gen: String => A): Props[A] = propsT[Id, A](path.toUri.toURL)(gen)
  @inline def props[A](uri: URI)(gen: String => A): Props[A]   = propsT[Id, A](uri.toURL)(gen)
  @inline def props[A](url: URL)(gen: String => A): Props[A]   = propsT[Id, A](url)(gen)

  @inline def propsT[F[_], A](path: Path)(gen: String => F[A]): PropsT[F, A] = propsT[F, A](path.toUri.toURL)(gen)
  @inline def propsT[F[_], A](uri: URI)(gen: String => F[A]): PropsT[F, A]   = propsT[F, A](uri.toURL)(gen)
  @inline def propsT[F[_], A](url: URL)(gen: String => F[A]): PropsT[F, A]   = new PropsT[F, A](url, gen)
}

private[trembita] class FileInputF[F[_]](implicit ctgF: ClassTag[F[_]]) extends InputT[F, Sequential, FileInput.PropsT[F, ?]] {
  private implicit def factg[A: ClassTag]: ClassTag[F[A]] =
    ClassTag[F[A]](ctgF.runtimeClass)

  def create[A: ClassTag](props: FileInput.PropsT[F, A])(
      implicit F: Monad[F]
  ): DataPipelineT[F, A, Sequential] =
    new StrictSource[F, F[A]](F.pure(Source.fromURL(props.url).getLines().map(props.gen)), F)
      .mapMImpl[F[A], A](fa => fa)
}
