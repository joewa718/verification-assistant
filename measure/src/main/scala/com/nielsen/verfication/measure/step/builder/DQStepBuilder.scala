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

import com.nielsen.verfication.measure.Loggable
import com.nielsen.verfication.measure.configuration.dqdefinition.{DataSourceParam, Param, RuleParam}
import com.nielsen.verfication.measure.configuration.enums.{BatchProcessType, DataFrameOpsType, DslType, GriffinDslType, ProcessType, SparkSqlType, StreamingProcessType}
import com.nielsen.verfication.measure.context.DQContext
import com.nielsen.verfication.measure.step.DQStep
import org.apache.commons.lang.StringUtils
import com.nielsen.verfication.measure.configuration.dqdefinition.Param
import com.nielsen.verfication.measure.configuration.enums._
import com.nielsen.verfication.measure.step._

/**
  * build dq step by param
  */
trait DQStepBuilder extends Loggable with Serializable {

  type ParamType <: Param

  def buildDQStep(context: DQContext, param: ParamType): Option[DQStep]

  protected def getStepName(name: String): String = {
    if (StringUtils.isNotBlank(name)) name
    else DQStepNameGenerator.genName
  }

}

object DQStepBuilder {

  def buildStepOptByDataSourceParam(context: DQContext, dsParam: DataSourceParam
                                   ): Option[DQStep] = {
    getDataSourceParamStepBuilder(context.procType)
      .flatMap(_.buildDQStep(context, dsParam))
  }

  private def getDataSourceParamStepBuilder(procType: ProcessType)
  : Option[DataSourceParamStepBuilder] = {
    procType match {
      case BatchProcessType => Some(BatchDataSourceStepBuilder())
      case StreamingProcessType => Some(StreamingDataSourceStepBuilder())
      case _ => None
    }
  }

  def buildStepOptByRuleParam(context: DQContext, ruleParam: RuleParam
                             ): Option[DQStep] = {
    val dslType = ruleParam.getDslType
    val dsNames = context.dataSourceNames
    val funcNames = context.functionNames
    val dqStepOpt = getRuleParamStepBuilder(dslType, dsNames, funcNames)
      .flatMap(_.buildDQStep(context, ruleParam))
    dqStepOpt.toSeq.flatMap(_.getNames).foreach(name =>
      context.compileTableRegister.registerTable(name)
    )
    dqStepOpt
  }

  private def getRuleParamStepBuilder(dslType: DslType, dsNames: Seq[String], funcNames: Seq[String]
                                     ): Option[RuleParamStepBuilder] = {
    dslType match {
      case SparkSqlType => Some(SparkSqlDQStepBuilder())
      case DataFrameOpsType => Some(DataFrameOpsDQStepBuilder())
      case GriffinDslType => Some(GriffinDslDQStepBuilder(dsNames, funcNames))
      case _ => None
    }
  }

}
