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
import com.nielsen.verfication.measure.step.transform.DataFrameOpsTransformStep

case class DataFrameOpsDQStepBuilder() extends RuleParamStepBuilder {

  def buildSteps(context: DQContext, ruleParam: RuleParam): Seq[DQStep] = {
    val name = getStepName(ruleParam.getOutDfName())
    val inputDfName = getStepName(ruleParam.getInDfName())
    val transformStep = DataFrameOpsTransformStep(
      name, inputDfName, ruleParam.getRule, ruleParam.getDetails, ruleParam.getCache)
    transformStep +: (buildAssertSteps(ruleParam) ++ buildDirectWriteSteps(ruleParam))
  }

}
