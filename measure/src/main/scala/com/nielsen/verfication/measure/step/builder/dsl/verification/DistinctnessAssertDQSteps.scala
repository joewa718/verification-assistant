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
package com.nielsen.verfication.measure.step.builder.dsl.verification

import com.nielsen.verfication.measure.configuration.dqdefinition.RuleParam
import com.nielsen.verfication.measure.context.DQContext

/**
 * generate accuracy assert steps
 */
case class DistinctnessAssertDQSteps(context: DQContext, ruleParam: RuleParam) extends Assert2DQSteps {

  override def execute(context: DQContext): Boolean = {
    context.metricWrapper.flush.foldLeft(true) { (ret, pair) =>
      val (t, metric) = pair
      val value = metric.get("value").get.asInstanceOf[Map[String, Any]]
      val total = value.get("total").get.asInstanceOf[Int]
      val distinct = value.get("distinct").get.asInstanceOf[Int]
      if (total != distinct) {
        context.messageSeq.append(context.name + "-" + context.contextId.id + "-" + ruleParam.getOutDfName())
      }
      true
    }
  }
}
