//package com.datarootlabs.trembita
//
//import org.slf4j.Logger
//
//package object slf4j {
//  implicit class LoggingOps[A](val self: DataPipeline[A]) extends AnyVal {
//    def withLogger(logger: Logger): DataPipeline[A] = LoggedSource(logger)(self)
//  }
//}
