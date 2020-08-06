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
package org.apache.griffin.measure.lancher

import com.nielsen.verfication.measure.Application
import com.nielsen.verfication.measure.configuration.dqdefinition.DQConfig
import com.nielsen.verfication.measure.configuration.dqdefinition.reader.{ParamJsonReader, ParamReader}
import com.nielsen.verfication.measure.utils.HdfsUtil
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Failure, Success}

class ApplicationSpec extends FlatSpec with Matchers {

  it should ("verification_batch_sparksql") in {
    val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", "file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/test/resources/verification_batch_sparksql.json")
    Application.run(args)
  }

  it should ("_completeness-batch-griffindsl.json") in {
    val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", "file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/test/resources/_completeness-batch-griffindsl.json")
    Application.run(args)
  }

  it should ("_assert-batch-griffindsl.json") in {
    val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", "file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/test/resources/_assert-batch-griffindsl.json")
    val messageSeq = Application.run(args)
    println(messageSeq.mkString(","))
  }

  it should ("_distinctness-batch-griffindsl.json") in {
    val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", "file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/test/resources/_distinctness-batch-griffindsl.json")
    Application.run(args)
  }

  it should ("_timeliness-batch-griffindsl.json") in {
    val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", "file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/test/resources/_timeliness-batch-griffindsl.json")
    Application.run(args)
  }

  it should ("_uniqueness-batch-griffindsl.json") in {
    val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", "file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/test/resources/_uniqueness-batch-griffindsl.json")
    Application.run(args)
  }

  it should ("Launcher") in {
    val messageSeq = new ArrayBuffer[String]
    val configs = HdfsUtil.listSubPathsByType("file:///Users/zhanwa01/hdfs/config/", "file", true)
    configs.foreach(config => {
      val args = Array("file:///Users/zhanwa01/git-hub/verification-assistant/measure/src/main/resources/env-batch.json", config)
      val messages = Application.run(args)
      messageSeq.appendAll(messages)
    })
    messageSeq.foreach(message => {
      println(message)
    })
    HdfsUtil.deleteHdfsPath("file:///Users/zhanwa01/hdfs/report/20200806.txt")
    HdfsUtil.appendContent("file:///Users/zhanwa01/hdfs/report/20200806.txt", messageSeq.mkString("\n"))

  }
}


