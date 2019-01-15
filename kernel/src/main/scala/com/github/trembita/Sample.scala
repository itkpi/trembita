package com.github.trembita

import cats.Id

object Sample {
  DataPipelineT[Id, Sequential].create(Seq(1, 2, 3))

  DataPipelineT[Id, Sequential](empty).create()

  import cats.instances.try_._
  import scala.util.Try

  DataPipelineT[Try, Sequential].create(Try(Vector(1, 2, 3)))
}
