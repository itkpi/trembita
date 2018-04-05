//package com.datarootlabs.trembitason
//
//import io.circe._
//import io.circe.syntax._
//import com.datarootlabs.trembita.ql._
//import ArbitraryGroupResult._
//import Aggregation._
//import GroupingCriteria._
//import cats.Show
//import io.circe._
//import io.circe.syntax._
//import language.experimental.macros
//import scala.reflect.macros.blackbox
//
//
//trait instances {
//  def `##@-json`[A: Encoder]: Encoder[##@[A]] = new Encoder[##@[A]] {
//    override def apply(a: ##@[A]): Json = a.records.asJson
//  }
//
//  def `##@-decode`[A: Decoder]: Decoder[##@[A]] = Decoder.instance { c ⇒
//    c.as[Seq[A]].right.map(##@(_: _*))
//  }
//
//  def encodeTaggedImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](c: blackbox.Context): c.Expr[Encoder[A ## U]] = {
//    import c.universe._
//
//    val A = weakTypeOf[A].dealias
//    val u = weakTypeOf[U]
//    val U = u.dealias
//
//    c.Expr[Encoder[A ## U]](q"""
//      new Encoder[$A ## $U] {
//        private val valueEncoder = Encoder[$A]
//        override def apply(a: $A ## $U): Json = Json.obj(
//          "value" -> valueEncoder(a.value),
//          "tag"   -> Json.fromString(..${U.toString})
//        )
//      }
//    """)
//  }
//
//  def decodeTaggedImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](c: blackbox.Context): c.Expr[Decoder[A ## U]] = {
//    import c.universe._
//
//    val A = weakTypeOf[A].dealias
//    val u = weakTypeOf[U]
//    val U = u.dealias
//
//    c.Expr[Decoder[A ## U]](q"""
//      Decoder.instance { c ⇒
//        val valueF = c.downField("value").as[$A]
//        val tagF = c.get[String]("tag")
//        val result: Decoder.Result[$A ## $U] = tagF match {
//          case Right(tag) if tag == ..${U.toString} => valueF match {
//            case Right(v) => Right(v.as[$U])
//            case Left(err) => Left(err)
//          }
//          case Left(err) => Left(err)
//        }
//        result
//      }
//    """)
//  }
//
//  object AgNilEncoder extends Encoder[AgNil] {
//    override def apply(a: AgNil): Json = Json.fromString("AgNil")
//  }
//
//  object AgNilDecoder extends Decoder[AgNil] {
//    override def apply(c: HCursor): Decoder.Result[AgNil] = c.value.asString match {
//      case Some("AgNil") ⇒ Right(AgNil)
//      case Some(other)   ⇒ Left(DecodingFailure(s"$other is not a GNil", c.history))
//      case _             ⇒ Left(DecodingFailure("Not a GNil", c.history))
//    }
//  }
//
//  def AggregationEncoder[KH <: ##[_, _], KT <: Aggregation]
//  (implicit headEncoder: Encoder[KH],
//   tailEncoder: Encoder[KT]): Encoder[KH %:: KT] = new Encoder[KH %:: KT] {
//    override def apply(a: KH %:: KT): Json = {
//      val headEncoded = headEncoder(a.agHead)
//      val tailEncoded = tailEncoder(a.agTail)
//      tailEncoded.asArray match {
//        case Some(arr) ⇒ Json.arr(headEncoded +: arr: _*)
//        case _         ⇒ Json.arr(headEncoded, tailEncoded)
//      }
//    }
//  }
//
//  def AggregationDecoder[KH <: ##[_, _], KT <: Aggregation]
//  (implicit headDecoder: Decoder[KH],
//   tailDecoder: Decoder[KT]): Decoder[KH %:: KT] = new Decoder[KH %:: KT] {
//    override def apply(c: HCursor): Decoder.Result[KH %:: KT] = c.value.asArray match {
//      case Some(values) if values.lengthCompare(2) >= 0 ⇒
//        val head = values.head
//        val tail = values.tail
//        for {
//          kh ← headDecoder(head.hcursor).right
//          kt ← tailDecoder(
//            if (tail.lengthCompare(1) == 0) tail.head.hcursor
//            else Json.arr(tail: _*).hcursor
//          ).right
//        } yield kh %:: kt
//    }
//  }
//
//  def AggEncoderImpl[K <: Aggregation : c.WeakTypeTag](c: blackbox.Context): c.Expr[Encoder[K]] = {
//    import c.universe._
//
//    val K = weakTypeOf[K].dealias
//    val agnil = typeOf[AgNil].dealias
//
//    val expr = K match {
//      case `agnil` ⇒ q"AgNilEncoder"
//      case _       ⇒ K.typeArgs match {
//        case List(h, t) ⇒ q"AggregationEncoder[$h, $t]"
//      }
//    }
//    c.Expr[Encoder[K]](expr)
//  }
//
//  def AggDecoderImpl[K <: Aggregation : c.WeakTypeTag](c: blackbox.Context): c.Expr[Decoder[K]] = {
//    import c.universe._
//
//    val K = weakTypeOf[K].dealias
//    val agnil = typeOf[AgNil].dealias
//
//    val expr = K match {
//      case `agnil` ⇒ q"AgNilDecoder"
//      case _       ⇒ K.typeArgs match {
//        case List(h, t) ⇒ q"AggregationDecoder[$h, $t]"
//      }
//    }
//    c.Expr[Decoder[K]](expr)
//  }
//
//  object GNilEncoder extends Encoder[GNil] {
//    override def apply(a: GNil): Json = Json.fromString("GNil")
//  }
//
//  object GNilDecoder extends Decoder[GNil] {
//    override def apply(c: HCursor): Decoder.Result[GNil] = c.value.asString match {
//      case Some("GNil") ⇒ Right(GNil)
//      case Some(other)  ⇒ Left(DecodingFailure(s"$other is not a GNil", c.history))
//      case _            ⇒ Left(DecodingFailure("Not a GNil", c.history))
//    }
//  }
//
//  def GroupingCriteriaEncoder[KH <: ##[_, _], KT <: GroupingCriteria]
//  (implicit headEncoder: Encoder[KH],
//   tailEncoder: Encoder[KT]): Encoder[KH &:: KT] = new Encoder[KH &:: KT] {
//    override def apply(a: KH &:: KT): Json = {
//      val headEncoded = headEncoder(a.first)
//      val tailEncoded = tailEncoder(a.rest)
//      tailEncoded.asArray match {
//        case Some(arr) ⇒ Json.arr(headEncoded +: arr: _*)
//        case _         ⇒ Json.arr(headEncoded, tailEncoded)
//      }
//    }
//  }
//
//  def GroupingCriteriaDecoder[KH <: ##[_, _], KT <: GroupingCriteria]
//  (implicit headDecoder: Decoder[KH],
//   tailDecoder: Decoder[KT]): Decoder[KH &:: KT] = new Decoder[KH &:: KT] {
//    override def apply(c: HCursor): Decoder.Result[KH &:: KT] = c.value.asArray match {
//      case Some(values) if values.lengthCompare(2) >= 0 ⇒
//        val head = values.head
//        val tail = values.tail
//        for {
//          kh ← headDecoder(head.hcursor).right
//          kt ← tailDecoder(
//            if (tail.lengthCompare(1) == 0) tail.head.hcursor
//            else Json.arr(tail: _*).hcursor
//          ).right
//        } yield kh &:: kt
//    }
//  }
//
//  def grCrEncoderImpl[K <: GroupingCriteria : c.WeakTypeTag](c: blackbox.Context): c.Expr[Encoder[K]] = {
//    import c.universe._
//
//    val K = weakTypeOf[K].dealias
//    val gnil = typeOf[GNil].dealias
//
//    val expr = K match {
//      case `gnil` ⇒ q"GNilEncoder"
//      case _      ⇒ K.typeArgs match {
//        case List(h, t) ⇒ q"GroupingCriteriaEncoder[$h, $t]"
//      }
//    }
//    c.Expr[Encoder[K]](expr)
//  }
//
//  def grCrDecoderImpl[K <: GroupingCriteria : c.WeakTypeTag](c: blackbox.Context): c.Expr[Decoder[K]] = {
//    import c.universe._
//
//    val K = weakTypeOf[K].dealias
//    val gnil = typeOf[GNil].dealias
//
//    val expr = K match {
//      case `gnil` ⇒ q"GNilDecoder"
//      case _      ⇒ K.typeArgs match {
//        case List(h, t) ⇒ q"GroupingCriteriaDecoder[$h, $t]"
//      }
//    }
//    c.Expr[Decoder[K]](expr)
//  }
//
//  def circeEncoderImpl[A: c.WeakTypeTag, K <: GroupingCriteria : c.WeakTypeTag, T <: Aggregation : c.WeakTypeTag]
//  (c: blackbox.Context): c.Expr[Encoder[ArbitraryGroupResult[A, K, T]]] = {
//    import c.universe._
//
//    val A = weakTypeOf[A].dealias
//    val K = weakTypeOf[K].dealias
//    val T = weakTypeOf[T].dealias
//    val gnil = typeOf[GNil].dealias
//
//    val expr = K match {
//      case `gnil` ⇒ q"`##@-json`[$A]"
//      case _      ⇒ K.typeArgs match {
//        case List(kH, kT) ⇒ q"""
//            import io.circe._
//            import io.circe.syntax._
//            import com.datarootlabs.trembita.ql.ArbitraryGroupResult._
//
//            new Encoder[ArbitraryGroupResult[$A, $K, $T]] {
//              override def apply(a: ArbitraryGroupResult[$A, $K, $T]): Json = a match {
//                case cons: ~::[_,_,_,_] => Json.obj(
//                  "key" → cons.key.asInstanceOf[$kH].asJson,
//                  "totals" → cons.totals.asInstanceOf[$T].asJson,
//                  "subGroup" → cons.subGroup.asInstanceOf[ArbitraryGroupResult[$A, $kT, $T]].asJson
//                )
//                case mul: ~**[_,_, _] => Json.arr(
//                  mul.records.asInstanceOf[Seq[ArbitraryGroupResult[$A, $K, $T]]].map(this.apply(_)) :_*
//                )
//              }
//            }
//            """
//
//      }
//    }
//    c.Expr[Encoder[ArbitraryGroupResult[A, K, T]]](q"$expr.asInstanceOf[Encoder[ArbitraryGroupResult[$A, $K, $T]]]")
//  }
//
//  def circeDecoderImpl[A: c.WeakTypeTag, K <: GroupingCriteria : c.WeakTypeTag, T <: Aggregation : c.WeakTypeTag]
//  (c: blackbox.Context): c.Expr[Decoder[ArbitraryGroupResult[A, K, T]]] = {
//    import c.universe._
//
//    val A = weakTypeOf[A].dealias
//    val K = weakTypeOf[K].dealias
//    val T = weakTypeOf[T].dealias
//    val gnil = typeOf[GNil].dealias
//
//    val expr = K match {
//      case `gnil` ⇒ q"`##@-decode`[$A]"
//      case _      ⇒ K.typeArgs match {
//        case List(kH, kT) ⇒ q"""
//            import io.circe._
//            import io.circe.syntax._
//            import com.datarootlabs.trembita.ql.ArbitraryGroupResult._
//
//            new Decoder[ArbitraryGroupResult[$A, $K, $T]] {
//              def apply(c: HCursor): Decoder.Result[ArbitraryGroupResult[$A, $K, $T]] = c.value.asArray match {
//                case Some(values) =>
//                  val res = values.map(v => this.apply(v.hcursor))
//                    .foldLeft[Decoder.Result[Vector[ArbitraryGroupResult[$A, $K, $T]]]](Right(Vector.empty)) {
//                      case (Left(err), _) ⇒ Left(err)
//                      case (Right(acc), Left(err)) ⇒ Left(err)
//                      case (Right(acc), Right(elem)) ⇒ Right(acc :+ elem)
//                    }
//                  res.right.map(~**(_: _*))
//
//                case None =>
//                  val keyF = c.downField("key").as[$kH]
//                  val totalsF = c.downField("totals").as[$T]
//                  val subGroupsF = c.downField("subGroup").as[ArbitraryGroupResult[$A, $kT, $T]]
//                  for {
//                    key ← keyF
//                    totals ← totalsF
//                    subGroup ← subGroupsF
//                  } yield ~::(key, totals, subGroup)
//              }
//            }
//            """
//
//      }
//    }
//    c.Expr[Decoder[ArbitraryGroupResult[A, K, T]]](q"$expr.asInstanceOf[Decoder[ArbitraryGroupResult[$A, $K, $T]]]")
//  }
//}
//
//object instances extends instances {
//  implicit def taggedEncoder[A, U]: Encoder[A ## U] = macro encodeTaggedImpl[A, U]
//  implicit def taggedDecoder[A, U]: Decoder[A ## U] = macro decodeTaggedImpl[A, U]
//  implicit def aggregationDecoder[K <: Aggregation]: Decoder[K] = macro AggDecoderImpl[K]
//  implicit def aggregationEncoder[K <: Aggregation]: Encoder[K] = macro AggEncoderImpl[K]
//  implicit def groupingCriteriaDecoder[K <: GroupingCriteria]: Decoder[K] = macro grCrDecoderImpl[K]
//  implicit def groupingCriteriaEncoder[K <: GroupingCriteria]: Encoder[K] = macro grCrEncoderImpl[K]
//
//  implicit def arbitraryGroupResultEncoder[A, K <: GroupingCriteria, T <: Aggregation]
//  : Encoder[ArbitraryGroupResult[A, K, T]] = macro circeEncoderImpl[A, K, T]
//
//  implicit def arbitraryGroupResultDecoder[A, K <: GroupingCriteria, T <: Aggregation]
//  : Decoder[ArbitraryGroupResult[A, K, T]] = macro circeDecoderImpl[A, K, T]
//}
