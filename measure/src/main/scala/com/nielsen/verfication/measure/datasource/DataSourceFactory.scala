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
package com.nielsen.verfication.measure.datasource

import com.nielsen.verfication.measure.Loggable
import com.nielsen.verfication.measure.configuration.dqdefinition.DataSourceParam
import com.nielsen.verfication.measure.datasource.cache.StreamingCacheClientFactory
import com.nielsen.verfication.measure.datasource.connector.{DataConnector, DataConnectorFactory}

import scala.util.Success
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import com.nielsen.verfication.measure.datasource.connector.DataConnector


object DataSourceFactory extends Loggable {

  def getDataSources(sparkSession: SparkSession,
                     ssc: StreamingContext,
                     dataSources: Seq[DataSourceParam]
                    ): Seq[DataSource] = {
    dataSources.zipWithIndex.flatMap { pair =>
      val (param, index) = pair
      getDataSource(sparkSession, ssc, param, index)
    }
  }

  private def getDataSource(sparkSession: SparkSession,
                            ssc: StreamingContext,
                            dataSourceParam: DataSourceParam,
                            index: Int
                           ): Option[DataSource] = {
    val name = dataSourceParam.getName
    val connectorParams = dataSourceParam.getConnectors
    val timestampStorage = TimestampStorage()

    // for streaming data cache
    val streamingCacheClientOpt = StreamingCacheClientFactory.getClientOpt(
      sparkSession.sqlContext, dataSourceParam.getCheckpointOpt, name, index, timestampStorage)

    val dataConnectors: Seq[DataConnector] = connectorParams.flatMap { connectorParam =>
      DataConnectorFactory.getDataConnector(sparkSession, ssc, connectorParam,
        timestampStorage, streamingCacheClientOpt) match {
          case Success(connector) => Some(connector)
          case _ => None
        }
    }

    Some(DataSource(name, dataSourceParam, dataConnectors, streamingCacheClientOpt))
  }

}
