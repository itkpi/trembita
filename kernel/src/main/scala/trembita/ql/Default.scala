package trembita.ql

import cats.Monoid
import java.time._
import scala.annotation.implicitNotFound

@implicitNotFound("""
No found implicit value: Default[${A}]
Please add imports:
{{{
  import trembita.ql._
  import cats.implicits._
}}}
Or provide a default value for type ${A}
""")
trait Default[A] extends Serializable {
  def get: A
}

trait LowPriorityDefaults extends Serializable {
  implicit def defaultFromMonoid[A](implicit A: Monoid[A]): Default[A] =
    new Default[A] {
      val get: A = A.empty
    }

  implicit object DefaultDate extends Default[LocalDate] {
    val get: LocalDate = LocalDate.MIN
  }

  implicit object DefaultTime extends Default[LocalTime] {
    val get: LocalTime = LocalTime.MIN
  }

  implicit object DefaultDateTime extends Default[LocalDateTime] {
    val get: LocalDateTime = LocalDateTime.MIN
  }

}

object Default extends LowPriorityDefaults
