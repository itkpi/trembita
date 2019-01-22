package trembita.ql

import scala.language.higherKinds
import cats._

trait monoidInstances extends Serializable {

  /**
    * [[Monoid]] for some [[AggFunc.Result]] of [[A]]
    * producing [[R]] with combiner [[Comb]]
    *
    * @param AggF - aggregation function for the following types
    **/
  implicit def aggResMonoid[A, R, Comb](
      implicit AggF: AggFunc[A, R, Comb]
  ): Monoid[AggFunc.Result[A, R, Comb]] =
    new Monoid[AggFunc.Result[A, R, Comb]] {
      def empty: AggFunc.Result[A, R, Comb] = AggF.extract(AggF.empty)
      def combine(x: AggFunc.Result[A, R, Comb], y: AggFunc.Result[A, R, Comb]): AggFunc.Result[A, R, Comb] =
        AggF.extract(AggF.combine(x.combiner, y.combiner))
    }
}
