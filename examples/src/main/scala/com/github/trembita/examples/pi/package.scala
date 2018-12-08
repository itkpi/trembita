package com.github.trembita.examples

import cats.free.Free
import hammock._

package object pi {
  type HttpRequestF = Free[HttpF, HttpResponse]
}
