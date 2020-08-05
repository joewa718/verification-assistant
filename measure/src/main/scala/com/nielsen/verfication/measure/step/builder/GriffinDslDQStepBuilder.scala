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
package com.nielsen.verfication.measure.step.builder

import com.nielsen.verfication.measure.configuration.dqdefinition.RuleParam
import com.nielsen.verfication.measure.context.DQContext
import com.nielsen.verfication.measure.step.DQStep
import com.nielsen.verfication.measure.step.builder.dsl.verification.Assert2DQSteps
import com.nielsen.verfication.measure.step.builder.dsl.parser.GriffinDslParser
import com.nielsen.verfication.measure.step.builder.dsl.transform.Expr2DQSteps


case class GriffinDslDQStepBuilder(dataSourceNames: Seq[String],
                                   functionNames: Seq[String]
                                  ) extends RuleParamStepBuilder {

  val filteredFunctionNames = functionNames.filter { fn =>
    fn.matches("""^[a-zA-Z_]\w*$""")
  }
  val parser = GriffinDslParser(dataSourceNames, filteredFunctionNames)

  def buildSteps(context: DQContext, ruleParam: RuleParam): Seq[DQStep] = {
    val name = getStepName(ruleParam.getOutDfName())
    val rule = ruleParam.getRule
    val dqType = ruleParam.getDqType
    try {
      val result = parser.parseRule(rule, dqType)
      if (result.successful) {
        val expr = result.get
        val expr2DQSteps = Expr2DQSteps(context, expr, ruleParam.replaceOutDfName(name))
        val assert2DqStep = Assert2DQSteps(context,ruleParam)
        expr2DQSteps.getDQSteps() :+ assert2DqStep
      } else {
        warn(s"parse rule [ ${rule} ] fails: \n${result}")
        Nil
      }
    } catch {
      case e: Throwable =>
        error(s"generate rule plan ${name} fails: ${e.getMessage}", e)
        Nil
    }
  }

}
