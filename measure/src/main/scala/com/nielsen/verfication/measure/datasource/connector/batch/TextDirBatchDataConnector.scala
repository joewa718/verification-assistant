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
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.nielsen.verfication.measure.utils.ParamUtil._

/**
  * batch data connector for directory with text format data in the nth depth sub-directories
  */
case class TextDirBatchDataConnector(@transient sparkSession: SparkSession,
                                     dcParam: DataConnectorParam,
                                     timestampStorage: TimestampStorage
                                    ) extends BatchDataConnector {

  val config = dcParam.getConfig

  val DirPath = "dir.path"
  val DataDirDepth = "data.dir.depth"
  val SuccessFile = "success.file"
  val DoneFile = "done.file"

  val dirPath = config.getString(DirPath, "")
  val dataDirDepth = config.getInt(DataDirDepth, 0)
  val successFile = config.getString(SuccessFile, "_SUCCESS")
  val doneFile = config.getString(DoneFile, "_DONE")

  val ignoreFilePrefix = "_"

  private def dirExist(): Boolean = {
    HdfsUtil.existPath(dirPath)
  }

  def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = try {
      val dataDirs = listSubDirs(dirPath :: Nil, dataDirDepth, readable)
      // touch done file for read dirs
      val validDataDirs = dataDirs.filter(dir => !emptyDir(dir))
      if (validDataDirs.nonEmpty) {
        val df = sparkSession.read.option("header",true).csv(validDataDirs: _*)
        val dfOpt = Some(df)
        val preDfOpt = preProcess(dfOpt, ms)
        preDfOpt
      } else {
        None
      }
    } catch {
      case e: Throwable =>
        error(s"load text dir ${dirPath} fails: ${e.getMessage}", e)
        None
    }
    val tmsts = readTmst(ms)
    (dfOpt, TimeRange(ms, tmsts))
  }

  private def listSubDirs(paths: Seq[String],
                          depth: Int,
                          filteFunc: (String) => Boolean): Seq[String] = {
    val subDirs = paths.flatMap { path => HdfsUtil.listSubPathsByType(path, "file", true) }
    if (depth <= 0) {
      subDirs.filter(filteFunc)
    } else {
      listSubDirs(subDirs, depth - 1, filteFunc)
    }
  }

  private def readable(dir: String): Boolean = true

  private def touchDone(dir: String): Unit =
    HdfsUtil.createEmptyFile(HdfsUtil.getHdfsFilePath(dir, doneFile))

  private def emptyDir(dir: String): Boolean = {
    HdfsUtil.listSubPathsByType(dir, "file").filter(!_.startsWith(ignoreFilePrefix)).size == 0
  }

}
