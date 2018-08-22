/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import com.hortonworks.spark.sql.kafka08.KafkaSourceOffset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog
import org.apache.spark.sql.types.StructType

object SparkSqlUtils {

  def createStreamingDataFrame(
      context: SQLContext,
      rows: RDD[InternalRow],
      schema: StructType): DataFrame = {
    context.internalCreateDataFrame(rows, schema, isStreaming = true)
  }

  def parseVersion(metadataLog: HDFSMetadataLog[KafkaSourceOffset],
      text: String, maxSupportedVersion: Int): Int = {
    metadataLog.parseVersion(text, maxSupportedVersion)
  }
}
