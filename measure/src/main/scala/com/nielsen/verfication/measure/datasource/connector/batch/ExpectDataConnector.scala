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
package com.nielsen.verfication.measure.datasource.connector.batch

import com.nielsen.verfication.measure.configuration.dqdefinition.DataConnectorParam
import com.nielsen.verfication.measure.context.TimeRange
import com.nielsen.verfication.measure.datasource.TimestampStorage
import com.nielsen.verfication.measure.utils.HdfsUtil
import com.nielsen.verfication.measure.utils.ParamUtil._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * batch data connector for avro file
 */
case class ExpectDataConnector(@transient sparkSession: SparkSession,
                               dcParam: DataConnectorParam,
                               timestampStorage: TimestampStorage
                              ) extends BatchDataConnector {

  def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = try {
      val df = sparkSession.emptyDataFrame
      val dfOpt = Some(df)
      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    } catch {
      case e: Throwable =>
        None
    }
    val tmsts = readTmst(ms)
    (dfOpt, TimeRange(ms, tmsts))
  }


}
