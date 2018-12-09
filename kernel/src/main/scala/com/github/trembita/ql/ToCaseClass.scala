package com.github.trembita.ql

import shapeless._
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

trait ToCaseClass[A, K <: GroupingCriteria, T]
    extends DepFn1[QueryResult[A, K, T]]

object ToCaseClass {
  type Aux[A, K <: GroupingCriteria, T, Out0] = ToCaseClass[A, K, T] {
    type Out = Out0
  }

  def apply[A, K <: GroupingCriteria, T](
    implicit ev: ToCaseClass[A, K, T]
  ): Aux[A, K, T, ev.Out] = ev

  implicit def materialize[A, K <: GroupingCriteria, T, L <: HList, R](
    implicit ev: ToHList.Aux[QueryResult[A, K, T], L]
  ): ToCaseClass.Aux[A, K, T, R] =
    macro fromGenericImpl[A, K, T, L, R]

  def fromGenericImpl[A: c.WeakTypeTag,
                      K <: GroupingCriteria: c.WeakTypeTag,
                      T: c.WeakTypeTag,
                      L <: HList: c.WeakTypeTag,
                      R: c.WeakTypeTag](c: blackbox.Context)(
    ev: c.Expr[ToHList.Aux[QueryResult[A, K, T], L]]
  ): c.Expr[ToCaseClass.Aux[A, K, T, R]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val K = weakTypeOf[K].dealias
    val T = weakTypeOf[T].dealias
    val R = weakTypeOf[R].dealias
    val L = weakTypeOf[L].dealias
    val HList = typeOf[HList].dealias
    val :: = typeOf[::[_, _]].typeConstructor

    if (!R.typeSymbol.asClass.isCaseClass) {
      c.abort(c.enclosingPosition, s"$R is not a case class")
    }
    val rCaseClass = R.typeSymbol.asClass

    def decompose(hlist: Type): List[Type] = hlist.typeArgs match {
      case Nil              => Nil
      case List(head, tail) => head :: decompose(tail)
    }

    def extract(tpe: Type): List[Type] =
      if (isHList(tpe)) decompose(tpe) else Nil

    def isHList(tpe: Type): Boolean = tpe <:< HList

    def isCaseClass(tpe: Type): Boolean =
      tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass

    def traverse(t: ClassSymbol,
                 hlistT: Type,
                 hlistElemTypes: List[Type]): Tree = {
      val fields = t.info.decls.collect {
        case d if d.isMethod && d.asMethod.isCaseAccessor => d.asMethod
      }
      if (fields.forall(!_.returnType.typeSymbol.asClass.isCaseClass)) {
        q""" (hlist: $hlistT) => _root_.shapeless.Generic[$t].from(hlist) """
      } else {
        val casesWithRT: List[(Type, Tree)] =
          fields.map(f => f -> f.returnType).toList.zip(hlistElemTypes).map {
            case ((field, returnType), hlistElemType) =>
              val res =
                if (returnType.typeConstructor =:= typeOf[List[_]].typeConstructor) {
                  val listInnerType = returnType.typeArgs.head
                  val hlistInnerType = hlistElemType.typeArgs.head
                  val caseName = TermName(
                    s"at_${field.name.toString}_List${t.name.toString}_${listInnerType.typeSymbol.name.toString}"
                  )
                  if (isCaseClass(listInnerType)) {
                    val transformInner = traverse(
                      listInnerType.typeSymbol.asClass,
                      hlistInnerType,
                      extract(hlistInnerType)
                    )
                    q""" implicit def $caseName: Case.Aux[$hlistElemType, $returnType] = at[$hlistElemType](a => a.map($transformInner.andThen(hlist => _root_.shapeless.Generic[$listInnerType].from(hlist)))) """
                  } else {
                    q""" implicit def $caseName: Case.Aux[$hlistElemType, $returnType] = at[$hlistElemType](a => a)"""
                  }
                } else if (isCaseClass(returnType)) {
                  val rtAsClass = returnType.typeSymbol.asClass
                  val transformInner =
                    traverse(rtAsClass, hlistElemType, extract(hlistElemType))
                  val caseName =
                    TermName(
                      s"at_${field.name.toString}_${t.name.toString}_${rtAsClass.name.toString}"
                    )
                  q""" implicit def $caseName: Case.Aux[$hlistElemType, $returnType] = at[$hlistElemType](a => $transformInner(a)) """
                } else {
                  if (returnType =:= hlistElemType) {
                    val caseName = TermName(
                      s"at_${field.name.toString}_${t.name.toString}_${returnType.typeSymbol.name.toString}"
                    )
                    q""" implicit def $caseName: Case.Aux[$hlistElemType, $returnType] = at[$hlistElemType](a => a)"""
                  } else {
                    c.abort(
                      c.enclosingPosition,
                      s"$field return type $returnType (in case class $t) does not conform to hlist element type $hlistElemType"
                    )
                  }
                }
              returnType -> res
          }
        val cases: List[Tree] =
          casesWithRT.groupBy(_._1).map(_._2.head._2).toList
        val polyFuncName = TermName(s"poly_${t.name.toString}")
        q""" (hlist: $hlistT) => {
          object $polyFuncName extends shapeless.Poly1 {
            ..$cases
          }
          hlist.map($polyFuncName)
        }
       """
      }
    }

    val transform = traverse(rCaseClass, L, extract(L))

    val queryResult = tq"com.github.trembita.ql.QueryResult[$A, $K, $T]"
    c.Expr[ToCaseClass.Aux[A, K, T, R]](q"""
         new ToCaseClass[$A, $K, $T] {
           type Out = $R
           private val gen = shapeless.Generic[$R]
           def apply(in: $queryResult): $R = {
             val hlist = $ev(in)
             val prepared = $transform(hlist)
             gen.from(prepared)
           }
         }
       """)
  }
}
