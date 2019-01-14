package com.github.trembita.ql

case class QueryResult[+A, K <: GroupingCriteria, +T](keys: K, totals: T, values: Vector[A])
