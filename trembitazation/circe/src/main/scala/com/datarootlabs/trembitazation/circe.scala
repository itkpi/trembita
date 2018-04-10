package com.datarootlabs.trembitazation

import language.experimental.macros
import io.circe._
import io.circe.syntax._
import com.datarootlabs.trembita.ql._
import QueryResult._
import AggDecl._
import AggRes._
import GroupingCriteria._
import cats.Show
import cats.data.NonEmptyList
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._

import scala.reflect.macros.blackbox
import shapeless._


sealed trait circe {
  def `##@-json`[A: Encoder]: Encoder[##@[A]] = new Encoder[##@[A]] {
    override def apply(a: ##@[A]): Json = a.records.asJson
  }

  def `##@-decode`[A: Decoder]: Decoder[##@[A]] = Decoder.instance { c ⇒
    c.as[List[A]].right.map(##@(_))
  }

  def encodeTaggedImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](c: blackbox.Context): c.Expr[Encoder[A :@ U]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val u = weakTypeOf[U]
    val U = u.dealias

    c.Expr[Encoder[A :@ U]](q"""
      new Encoder[$A :@ $U] {
        private val valueEncoder = Encoder[$A]
        override def apply(a: $A :@ $U): Json = Json.obj(
          "value" -> valueEncoder(a.value),
          "tag"   -> Json.fromString(..${U.toString})
        )
      }
    """)
  }

  def decodeTaggedImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](c: blackbox.Context): c.Expr[Decoder[A :@ U]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val u = weakTypeOf[U]
    val U = u.dealias

    c.Expr[Decoder[A :@ U]](q"""
      Decoder.instance { c ⇒
        val valueF = c.get[$A]("value")
        val tagF = c.get[String]("tag")
        val result: Decoder.Result[$A :@ $U] = tagF match {
          case Right(tag) if tag == ..${U.toString} => valueF match {
            case Right(v) => Right(v.as[$U])
            case Left(err) => Left(err)
          }
          case Left(err) => Left(err)
        }
        result
      }
    """)
  }
  def encodeTaggedAggImpl[
  A: c.WeakTypeTag,
  U: c.WeakTypeTag,
  AggT <: AggFunc.Type : c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[Encoder[TaggedAgg[A, U, AggT]]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val u = weakTypeOf[U]
    val U = u.dealias
    val AggT = weakTypeOf[AggT].dealias

    c.Expr[Encoder[TaggedAgg[A, U, AggT]]](q"""
      new Encoder[TaggedAgg[$A, $U, $AggT]] {
        private val valueEncoder = Encoder[$A]
        override def apply(a: $A :@ $U): Json = Json.obj(
          "value" -> valueEncoder(a.value),
          "tag"   -> Json.fromString(..${U.toString}),
          "aggregation_function" -> Json.fromString(..${AggT.toString})
        )
      }
    """)
  }

  def decodeTaggedAggImpl[
  A: c.WeakTypeTag,
  U: c.WeakTypeTag,
  AggT <: AggFunc.Type : c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[Decoder[TaggedAgg[A, U, AggT]]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val u = weakTypeOf[U]
    val U = u.dealias
    val AggT = weakTypeOf[AggT].dealias

    c.Expr[Decoder[TaggedAgg[A, U, AggT]]](q"""
      Decoder.instance { c ⇒
        val valueF = c.get[$A]("value")
        val tagF = c.get[String]("tag")
        val aggregationFunctionF = c.get[String]("aggregation_function")
        val result: Decoder.Result[$A :@ $U] = tagF match {
          case Right(tag) if tag == ..${U.toString} =>
          aggregationFunctionF match {
            case Right(aggF) if aggF == ..${AggT.toString} => valueF match {
              case Right(v) => Right(v.as[$U])
              case Left(err) => Left(err)
            }
            case Left(err) => Left(err)
          }
          case Left(err) => Left(err)
        }
        result
      }
    """)
  }

  implicit object HNilEncoder extends Encoder[HNil] {
    override def apply(a: HNil): Json = Json.fromString("HNil")
  }

  implicit object HNilDecoder extends Decoder[HNil] {
    override def apply(c: HCursor): Decoder.Result[HNil] = c.value.asString match {
      case Some("HNil") ⇒ Right(HNil)
      case Some(other)  ⇒ Left(DecodingFailure(s"$other is not a HNil", c.history))
      case _            ⇒ Left(DecodingFailure("Not a HNil", c.history))
    }
  }

  implicit def hlistEncoder[H, T <: HList](implicit headEncoder: Encoder[H], tailEncoder: Encoder[T]): Encoder[H :: T] =
    new Encoder[H :: T] {
      override def apply(a: H :: T): Json = {
        val headEncoded = headEncoder(a.head)
        val tailEncoded = tailEncoder(a.tail)
        tailEncoded.asArray match {
          case Some(arr) ⇒ Json.arr(headEncoded +: arr: _*)
          case _         ⇒ Json.arr(headEncoded, tailEncoded)
        }
      }
    }
  implicit def hlistDecoder[H, T <: HList]
  (implicit headDecoder: Decoder[H],
   tailDecoder: Decoder[T]): Decoder[H :: T] = new Decoder[H :: T] {
    override def apply(c: HCursor): Decoder.Result[H :: T] = c.value.asArray match {
      case Some(values) if values.lengthCompare(2) >= 0 ⇒
        val head = values.head
        val tail = values.tail
        for {
          kh ← headDecoder(head.hcursor).right
          kt ← tailDecoder(
            if (tail.lengthCompare(1) == 0) tail.head.hcursor
            else Json.arr(tail: _*).hcursor
          ).right
        } yield kh :: kt
      case _                                            ⇒ Left(DecodingFailure("not a HList", c.history))
    }
  }

  implicit object DNilEncoder extends Encoder[DNil] {
    override def apply(a: DNil): Json = Json.fromString("DNil")
  }

  implicit object DNilDecoder extends Decoder[DNil] {
    override def apply(c: HCursor): Decoder.Result[DNil] = c.value.asString match {
      case Some("DNil") ⇒ Right(DNil)
      case Some(other)  ⇒ Left(DecodingFailure(s"$other is not a DNil", c.history))
      case _            ⇒ Left(DecodingFailure("Not a DNil", c.history))
    }
  }

  implicit def aggDeclEncoder[H <: TaggedAgg[_, _, _], T <: AggDecl]
  (headEncoder: Encoder[H], tailEncoder: Encoder[T]): Encoder[H %:: T] =
    new Encoder[H %:: T] {
      override def apply(a: H %:: T): Json = {
        val headEncoded = headEncoder(a.head)
        val tailEncoded = tailEncoder(a.tail)
        tailEncoded.asArray match {
          case Some(arr) ⇒ Json.arr(headEncoded +: arr: _*)
          case _         ⇒ Json.arr(headEncoded, tailEncoded)
        }
      }
    }

  implicit def aggDeclDecoder[H <: TaggedAgg[_, _, _], T <: AggDecl]
  (implicit headDecoder: Decoder[H],
   tailDecoder: Decoder[T]): Decoder[H %:: T] = new Decoder[H %:: T] {
    override def apply(c: HCursor): Decoder.Result[H %:: T] = c.value.asArray match {
      case Some(values) if values.lengthCompare(2) >= 0 ⇒
        val head = values.head
        val tail = values.tail
        for {
          kh ← headDecoder(head.hcursor).right
          kt ← tailDecoder(
            if (tail.lengthCompare(1) == 0) tail.head.hcursor
            else Json.arr(tail: _*).hcursor
          ).right
        } yield kh %:: kt
      case _                                            ⇒ Left(DecodingFailure("not a HList", c.history))
    }
  }

  implicit object RNilEncoder extends Encoder[RNil] {
    override def apply(a: RNil): Json = Json.fromString("RNil")
  }

  implicit object RNilDecoder extends Decoder[RNil] {
    override def apply(c: HCursor): Decoder.Result[RNil] = c.value.asString match {
      case Some("RNil") ⇒ Right(RNil)
      case Some(other)  ⇒ Left(DecodingFailure(s"$other is not a RNil", c.history))
      case _            ⇒ Left(DecodingFailure("Not a RNil", c.history))
    }
  }

  implicit def AggResEncoder[KH <: :@[_, _], KT <: AggRes]
  (implicit headEncoder: Encoder[KH],
   tailEncoder: Encoder[KT]): Encoder[KH *:: KT] = new Encoder[KH *:: KT] {
    override def apply(a: KH *:: KT): Json = {
      val headEncoded = headEncoder(a.head)
      val tailEncoded = tailEncoder(a.tail)
      tailEncoded.asArray match {
        case Some(arr) ⇒ Json.arr(headEncoded +: arr: _*)
        case _         ⇒ Json.arr(headEncoded, tailEncoded)
      }
    }
  }

  implicit def AggResDecoder[KH <: :@[_, _], KT <: AggRes]
  (implicit headDecoder: Decoder[KH],
   tailDecoder: Decoder[KT]): Decoder[KH *:: KT] = new Decoder[KH *:: KT] {
    override def apply(c: HCursor): Decoder.Result[KH *:: KT] = c.value.asArray match {
      case Some(values) if values.lengthCompare(2) >= 0 ⇒
        val head = values.head
        val tail = values.tail
        for {
          kh ← headDecoder(head.hcursor).right
          kt ← tailDecoder(
            if (tail.lengthCompare(1) == 0) tail.head.hcursor
            else Json.arr(tail: _*).hcursor
          ).right
        } yield kh *:: kt
      case _                                            ⇒ Left(DecodingFailure("not an AggRes", c.history))
    }
  }

  implicit object GNilEncoder extends Encoder[GNil] {
    override def apply(a: GNil): Json = Json.fromString("GNil")
  }

  implicit object GNilDecoder extends Decoder[GNil] {
    override def apply(c: HCursor): Decoder.Result[GNil] = c.value.asString match {
      case Some("GNil") ⇒ Right(GNil)
      case Some(other)  ⇒ Left(DecodingFailure(s"$other is not a GNil", c.history))
      case _            ⇒ Left(DecodingFailure("Not a GNil", c.history))
    }
  }

  implicit def GroupingCriteriaEncoder[KH <: :@[_, _], KT <: GroupingCriteria]
  (implicit headEncoder: Encoder[KH],
   tailEncoder: Encoder[KT]): Encoder[KH &:: KT] = new Encoder[KH &:: KT] {
    override def apply(a: KH &:: KT): Json = {
      val headEncoded = headEncoder(a.head)
      val tailEncoded = tailEncoder(a.tail)
      tailEncoded.asArray match {
        case Some(arr) ⇒ Json.arr(headEncoded +: arr: _*)
        case _         ⇒ Json.arr(headEncoded, tailEncoded)
      }
    }
  }

  implicit def GroupingCriteriaDecoder[KH <: :@[_, _], KT <: GroupingCriteria]
  (implicit headDecoder: Decoder[KH],
   tailDecoder: Decoder[KT]): Decoder[KH &:: KT] = new Decoder[KH &:: KT] {
    override def apply(c: HCursor): Decoder.Result[KH &:: KT] = c.value.asArray match {
      case Some(values) if values.lengthCompare(2) >= 0 ⇒
        val head = values.head
        val tail = values.tail
        for {
          kh ← headDecoder(head.hcursor).right
          kt ← tailDecoder(
            if (tail.lengthCompare(1) == 0) tail.head.hcursor
            else Json.arr(tail: _*).hcursor
          ).right
        } yield kh &:: kt
      case _                                            ⇒ Left(DecodingFailure("not a GroupingCriteria", c.history))
    }
  }

  implicit def aggFuncResEncoder[A, Out: Encoder, Comb: Encoder]: Encoder[AggFunc.Result[A, Out, Comb]] =
    Encoder.instance {
      case AggFunc.Result(out, comb) ⇒
        Json.obj(
          "out" → out.asJson,
          "combiner" → comb.asJson
        )
    }

  implicit def aggFuncResDecoder[A, Out: Decoder, Comb: Decoder]: Decoder[AggFunc.Result[A, Out, Comb]] =
    Decoder.instance { c ⇒
      for {
        out ← c.get[Out]("out")
        comb ← c.get[Comb]("combiner")
      } yield AggFunc.Result[A, Out, Comb](out, comb)
    }

  implicit object stringBuilderEncoder extends Encoder[StringBuilder] {
    override def apply(a: StringBuilder): Json = Json.fromString(a.toString)
  }
  implicit object stringBuilderDecoder extends Decoder[StringBuilder] {
    override def apply(c: HCursor): Result[StringBuilder] = c.as[String].right.map(new StringBuilder(_))
  }

  implicit def keyEncoder[K: Encoder]: Encoder[Key[K]] = Encoder.instance {
    case Key.NoKey                ⇒ Json.fromString("NoKey")
    case Key.Single(value)        ⇒ Json.obj("key" → value.asJson)
    case Key.Multiple(head, tail) ⇒ Json.obj("keys" → Json.arr((head :: tail).asJson))
  }
  implicit def keyDecoder[K: Decoder]: Decoder[Key[K]] = Decoder.instance { c ⇒
    c.value.asString match {
      case Some("NoKey") ⇒ Right(Key.NoKey)
      case _             ⇒ c.get[K]("key") match {
        case Right(value) ⇒ Right(Key.Single(value))
        case Left(_)      ⇒ c.get[NonEmptyList[K]]("keys") match {
          case Right(NonEmptyList(head, scala.::(h2, tail))) ⇒ Right(Key.Multiple(head, NonEmptyList(h2, tail)))
          case Right(_)                                      ⇒ Left(DecodingFailure("Not enough keys for Key.Multiple", c.history))
          case Left(err)                                     ⇒ Left(err)
        }
      }
    }
  }


  def circeEncoderImpl[A: c.WeakTypeTag, K <: GroupingCriteria : c.WeakTypeTag, T: c.WeakTypeTag]
  (c: blackbox.Context): c.Expr[Encoder[QueryResult[A, K, T]]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val K = weakTypeOf[K].dealias
    val T = weakTypeOf[T].dealias
    val gnil = typeOf[GNil].dealias

    val expr = K match {
      case `gnil` ⇒ q"`##@-json`[$A]"
      case _      ⇒ K.typeArgs match {
        case List(kH, kT) ⇒ q"""
            import io.circe._
            import io.circe.syntax._
            import cats.data.NonEmptyList
            import com.datarootlabs.trembita.ql.QueryResult._

            new Encoder[QueryResult[$A, $K, $T]] {
              override def apply(a: QueryResult[$A, $K, $T]): Json = a match {
                case Empty(totals) => Json.obj("Empty" -> Json.obj(), "totals" -> totals.asInstanceOf[$T].asJson)
                case cons: ~::[_,_,_,_] => Json.obj(
                  "key" → cons.key.asInstanceOf[Key[$kH]].asJson,
                  "totals" → cons.totals.asInstanceOf[$T].asJson,
                  "subResult" → cons.subResult.asInstanceOf[QueryResult[$A, $kT, $T]].asJson
                )
                case mul: ~**[_,_, _] =>
                Json.obj(
                  "totals" -> mul.totals.asInstanceOf[$T].asJson,
                  "records" -> Json.arr(
                    mul.records.asInstanceOf[NonEmptyList[QueryResult[$A, $K, $T]]].map(this.apply(_)).toList :_*
                  )
                )
              }
            }
            """

      }
    }
    c.Expr[Encoder[QueryResult[A, K, T]]](q"$expr.asInstanceOf[Encoder[QueryResult[$A, $K, $T]]]")
  }

  def circeDecoderImpl[A: c.WeakTypeTag, K <: GroupingCriteria : c.WeakTypeTag, T: c.WeakTypeTag]
  (c: blackbox.Context): c.Expr[Decoder[QueryResult[A, K, T]]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val K = weakTypeOf[K].dealias
    val T = weakTypeOf[T].dealias
    val gnil = typeOf[GNil].dealias

    val expr = K match {
      case `gnil` ⇒ q"`##@-decode`[$A]"
      case _      ⇒ K.typeArgs match {
        case List(kH, kT) ⇒ q"""
            import io.circe._
            import io.circe.syntax._
            import cats.data.NonEmptyList
            import com.datarootlabs.trembita.ql.QueryResult._

            new Decoder[QueryResult[$A, $K, $T]] {
              def apply(c: HCursor): Decoder.Result[QueryResult[$A, $K, $T]] = {
                val emptyF: Decoder.Result[Empty[$A, $K, $T]] = for {
                  _ <- c.downField("Empty").focus.toRight(
                    DecodingFailure("Looks like QueryResult.Empty but missing 'Empty' field", c.history)
                  )
                  totals <- c.get[$T]("totals")
                } yield Empty(totals)

                val mulF: Decoder.Result[~**[$A, $K, $T]] = for {
                  totals <- c.get[$T]("totals")
                  NonEmptyList(head, scala.::(h2, tail)) <- c.get[NonEmptyList[QueryResult[$A, $K, $T]]]("records")
                } yield ~**(totals, head, h2 :: tail)

                val consF: Decoder.Result[~::[$A, $kH, $kT, $T]] = for {
                  key ← c.get[Key[$kH]]("key")
                  totals ← c.get[$T]("totals")
                  subResult ← c.get[QueryResult[$A, $kT, $T]]("subResult")
                } yield ~::(key, totals, subResult)

                emptyF match {
                  case Right(result) => Right(result)
                  case Left(_) => mulF match {
                    case Right(result) => Right(result)
                    case Left(_) => consF match {
                      case Right(result) => Right(result)
                      case Left(_) => Left(DecodingFailure("JSON is neither Empty nor ~:: nor ~**", c.history))
                    }
                  }
                }
              }
            }
            """

      }
    }
    c.Expr[Decoder[QueryResult[A, K, T]]](q"$expr.asInstanceOf[Decoder[QueryResult[$A, $K, $T]]]")
  }
}

object circe extends circe {
  implicit def taggedEncoder[A, U]: Encoder[A :@ U] = macro encodeTaggedImpl[A, U]
  implicit def taggedDecoder[A, U]: Decoder[A :@ U] = macro decodeTaggedImpl[A, U]

  implicit def encodeTaggedAgg[A, U, AggT <: AggFunc.Type]
  : Encoder[TaggedAgg[A, U, AggT]] = macro encodeTaggedAggImpl[A, U, AggT]
  implicit def decodeTaggedAgg[A, U, AggT <: AggFunc.Type]
  : Decoder[TaggedAgg[A, U, AggT]] = macro decodeTaggedAggImpl[A, U, AggT]

  //  implicit def aggregationEncoder[K <: Aggregation]: Encoder[K] = macro AggEncoderImpl[K]
  //  implicit def groupingCriteriaDecoder[K <: GroupingCriteria]: Decoder[K] = macro grCrDecoderImpl[K]
  //  implicit def groupingCriteriaEncoder[K <: GroupingCriteria]: Encoder[K] = macro grCrEncoderImpl[K]

  implicit def arbitraryGroupResultEncoder[A, K <: GroupingCriteria, T]
  : Encoder[QueryResult[A, K, T]] = macro circeEncoderImpl[A, K, T]

  implicit def arbitraryGroupResultDecoder[A, K <: GroupingCriteria, T]
  : Decoder[QueryResult[A, K, T]] = macro circeDecoderImpl[A, K, T]
}
