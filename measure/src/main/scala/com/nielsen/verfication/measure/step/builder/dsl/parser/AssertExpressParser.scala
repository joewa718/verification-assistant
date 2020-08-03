package com.nielsen.verfication.measure.step.builder.dsl.expr

import scala.util.parsing.combinator.RegexParsers

trait AssertExp {
  def matchResult(metricMaps: Seq[Map[String, Any]]):Boolean
}

case class AssertEqual(col: String, values: List[String]) extends AssertExp {
  override def matchResult(metricMaps: Seq[Map[String, Any]]): Boolean = {
    true
  }
}

case class AssertIn(col: String, values: List[String]) extends AssertExp {
  override def matchResult(metricMaps: Seq[Map[String, Any]]): Boolean = {
    true
  }
}

class AssertExpressParser extends RegexParsers {

  val IS: Parser[String] = """(?i)is\s""".r
  val IN: Parser[String] = """(?i)in\s""".r
  val col: Parser[String] = "(?i)[a-z]+".r
  val values: Parser[String] = "(?i)[a-z]+".r
  val LBR: Parser[String] = "("
  val RBR: Parser[String] = ")"
  val COMMA: Parser[String] = ","

  def equalExp: Parser[AssertEqual] = col ~ IS ~ LBR ~ repsep(values, COMMA) ~ RBR ^^ {
    case col ~ _ ~ _ ~ values ~ _ => AssertEqual(col, values)
  }

  def InExp: Parser[AssertIn] = col ~ IN ~ LBR ~ repsep(values, COMMA) ~ RBR ^^ {
    case col ~ _ ~ _ ~ values ~ _ => AssertIn(col, values)
  }

  def logicalFactor: Parser[AssertExp] = (equalExp | InExp) ^^ {
    case exp => exp
  }

  def getAssertExp(expression: String): ParseResult[AssertExp] = {
    parse(logicalFactor, expression)
  }

}

object AssertExpressParser extends AssertExpressParser {
  def main(args: Array[String]): Unit = {
    val test = getAssertExp("sex is (aaa,bb,cc)")
    println(test)
  }
}