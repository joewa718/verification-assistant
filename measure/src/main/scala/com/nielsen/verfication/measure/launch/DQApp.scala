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
package com.nielsen.verfication.measure.launch

import com.nielsen.verfication.measure.Loggable
import com.nielsen.verfication.measure.configuration.dqdefinition.{DQConfig, EnvConfig, SinkParam}

import scala.util.Try
import com.nielsen.verfication.measure.configuration.dqdefinition.SinkParam
import com.nielsen.verfication.measure.context.DQContext


/**
  * dq application process
  */
trait DQApp extends Loggable with Serializable {

  val envParam: EnvConfig
  val dqParam: DQConfig

  def init: Try[_]

  /**
    * @return execution success
    */
  def run: Try[(Boolean,DQContext)]

  def close: Try[_]

  /**
    * application will exit if it fails in run phase.
    * if retryable is true, the exception will be threw to spark env,
    * and enable retry strategy of spark application
    */
  def retryable: Boolean

  /**
    * timestamp as a key for metrics
    */
  protected def getMeasureTime: Long = {
    dqParam.getTimestampOpt match {
      case Some(t) if t > 0 => t
      case _ => System.currentTimeMillis
    }
  }

  protected def getSinkParams: Seq[SinkParam] = {
    val validSinkTypes = dqParam.getValidSinkTypes
    envParam.getSinkParams.flatMap { sinkParam =>
      if (validSinkTypes.contains(sinkParam.getType)) Some(sinkParam) else None
    }
  }

}
