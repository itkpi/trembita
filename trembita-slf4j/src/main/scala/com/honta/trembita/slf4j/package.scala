package com.honta.trembita


import org.slf4j.Logger

package object slf4j {
  implicit class LoggingOps[A](val self: LazyList[A]) extends AnyVal {
    def withLogger(logger: Logger): LazyList[A] = LoggedSource(logger)(self)
  }
}
