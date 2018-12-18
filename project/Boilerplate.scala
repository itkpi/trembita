import sbt._

/**
    * Copied, from https://github.com/milessabin/shapeless/blob/master/project/Boilerplate.scala
    *
    * Generate a range of boilerplate classes, those offering alternatives with 0-22 params
    * and would be tedious to craft by hand
    *
    * @author Miles Sabin
    * @author Kevin Wright
    */
  object Boilerplate {

    import scala.StringContext._

    implicit class BlockHelper(val sc: StringContext) extends AnyVal {
      def block(args: Any*): String = {
        val interpolated = sc.standardInterpolator(treatEscapes, args)
        val rawLines     = interpolated split '\n'
        val trimmedLines = rawLines map { _ dropWhile (_.isWhitespace) }
        trimmedLines mkString "\n"
      }
    }

    val templates: Seq[Template] = List(
      GenTuplerInstances
    )

    /** Returns a seq of the generated files.  As a side-effect, it actually generates them... */
    def gen(dir: File) = for (t <- templates) yield {
      val tgtFile = dir / "com" / "github" / "trembita" / "ql" / t.filename
      IO.write(tgtFile, t.body)
      tgtFile
    }

    class TemplateVals(val arity: Int) {
      val synTypes     = (0 until arity) map (n => (n + 'A').toChar)
      val synVals      = (0 until arity) map (n => (n + 'a').toChar)
      val synTypedVals = (synVals zip synTypes) map { case (v, t) => v + ":" + t }

      val `A..N`     = synTypes.mkString(", ")
      val `A..N,Res` = (synTypes :+ "Res") mkString ", "
      val `a..n`     = synVals.mkString(", ")
      val `A::N`     = (synTypes :+ "HNil") mkString "::"
      val `a::n`     = (synVals :+ "HNil") mkString "::"
      val `_.._`     = Seq.fill(arity)("_").mkString(", ")
      val `(A..N)`   = if (arity == 1) "Tuple1[A]" else synTypes.mkString("(", ", ", ")")
      val `(_.._)`   = if (arity == 1) "Tuple1[_]" else Seq.fill(arity)("_").mkString("(", ", ", ")")
      val `(a..n)`   = if (arity == 1) "Tuple1(a)" else synVals.mkString("(", ", ", ")")
      val `a:A..n:N` = synTypedVals mkString ", "
    }

    trait Template {
      def filename: String
      def content(tv: TemplateVals): String
      def range = 1 to 22
      def body: String = {
        val headerLines = header split '\n'
        val rawContents = range map { n =>
          content(new TemplateVals(n)) split '\n' filterNot (_.isEmpty)
        }
        val preBody   = rawContents.head takeWhile (_ startsWith "|") map (_.tail)
        val instances = rawContents flatMap { _ filter (_ startsWith "-") map (_.tail) }
        val postBody  = rawContents.head dropWhile (_ startsWith "|") dropWhile (_ startsWith "-") map (_.tail)
        (headerLines ++ preBody ++ instances ++ postBody) mkString "\n"
      }
    }

    object GenTuplerInstances extends Template {
      val filename = "tupler.scala"
      def content(tv: TemplateVals) = {
        import tv._
        block"""
             |package com.github.trembita.ql
             |
             |import hlist.Tupler
             |
             |trait TuplerInstances {
             |  type Aux[L <: HList, Out0] = Tupler[L] { type Out = Out0 }
        -
        -  implicit def hlistTupler${arity}
        -    [${`A..N`}]
        -  : Aux[
        -    ${`A::N`},
        -    ${`(A..N)`}
        -  ] =
        -    new Tupler[${`A::N`}] {
        -      type Out = ${`(A..N)`}
        -      def apply(l : ${`A::N`}): Out = l match { case ${`a::n`} => ${`(a..n)`} }
        -    }
             |}
      """
      }
    }
  }
