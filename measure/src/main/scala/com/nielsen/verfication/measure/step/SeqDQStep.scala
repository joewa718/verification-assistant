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
package com.nielsen.verfication.measure.step

import com.nielsen.verfication.measure.context.DQContext

/**
  * sequence of dq steps
  */
case class SeqDQStep(dqSteps: Seq[DQStep]) extends DQStep {

  val name: String = ""
  val rule: String = ""
  val details: Map[String, Any] = Map()

  /**
    * @return execution success
    */
  def execute(context: DQContext): Boolean = {
    dqSteps.foldLeft(true) { (ret, dqStep) =>
      ret && dqStep.execute(context)
    }
  }

  override def getNames(): Seq[String] = {
    dqSteps.foldLeft(Nil: Seq[String]) { (ret, dqStep) =>
      ret ++ dqStep.getNames
    }
  }

}
