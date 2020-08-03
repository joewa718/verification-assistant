/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package com.nielsen.verfication.measure.step.write

import com.nielsen.verfication.measure.configuration.dqdefinition.RuleAssertParam
import com.nielsen.verfication.measure.context.DQContext
import com.nielsen.verfication.measure.step.DQStep
import com.nielsen.verfication.measure.step.builder.dsl.expr.{AssertEqual, AssertExpressParser, AssertIn}
import com.nielsen.verfication.measure.utils.JsonUtil

/**
 * write metrics into context metric wrapper
 */
case class AssertExpressionStep(name: String, assert: Seq[RuleAssertParam], inputName: String) extends DQStep {

  def execute(context: DQContext): Boolean = {
    val metricMaps: Seq[Map[String, Any]] = getMetricMaps(context)
    assert.foreach(ruleAssertParam => {
      val expressSeq = ruleAssertParam.getAssertExpress()
      val assertExpressParser =  AssertExpressParser()
      val assertExpressVo = expressSeq.get.map(express => assertExpressParser.getAssertExp(express))
      for (express <- assertExpressVo) {
        val result = express match {
          case express: AssertEqual => express.matchResult(metricMaps)
          case express: AssertIn => express.matchResult(metricMaps)
        }
        result
      }
    })
    true
  }

  private def getMetricMaps(context: DQContext): Seq[Map[String, Any]] = {
    try {
      val pdf = context.sqlContext.table(s"`$inputName`")
      val records = pdf.toJSON.collect()
      if (records.length > 0) {
        records.flatMap { rec =>
          try {
            val value = JsonUtil.toAnyMap(rec)
            Some(value)
          } catch {
            case _: Throwable => None
          }
        }.toSeq
      } else Nil
    } catch {
      case e: Throwable =>
        error(s"get metric $name fails", e)
        Nil
    }
  }

}
