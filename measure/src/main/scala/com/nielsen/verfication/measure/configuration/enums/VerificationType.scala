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
package com.nielsen.verfication.measure.configuration.enums

import scala.util.matching.Regex

/**
  * the strategy to flatten metric
  */
sealed trait VerificationType {
  val idPattern: Regex
  val desc: String
}

object VerificationType {
  private val outputTypes: List[OutputType] = List(
    MetricOutputType,
    RecordOutputType,
    DscUpdateOutputType,
    UnknownOutputType
  )

  val default = UnknownOutputType
  def apply(ptn: String): OutputType = {
    outputTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(default)
  }
  def unapply(pt: OutputType): Option[String] = Some(pt.desc)
}
