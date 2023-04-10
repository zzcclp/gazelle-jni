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
package org.apache.spark.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.SparkEnv

import java.util.concurrent.ConcurrentHashMap

class GlutenDriverEndpoint extends IsolatedRpcEndpoint with Logging {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  rpcEnv.setupEndpoint(GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME, this)

  override def receive: PartialFunction[Any, Unit] = {
    super.receive
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case e@GlutenRegisterExecutor(executorId, executorRef) =>
      logError(s"------------ GlutenRegisterExecutor : ${executorId} : ${executorRef}")
      GlutenDriverEndpoint.this.synchronized {
        GlutenDriverEndpoint.executorDataMap.put(executorId, e)
      }
      context.reply(true)
  }

  override def onStart(): Unit = {
    logError(s"------------ start GlutenDriverEndpoint")
  }

  override def onStop(): Unit = {
    logError(s"------------ stop GlutenDriverEndpoint")
  }

}

object GlutenDriverEndpoint {

  lazy val glutenDriverEndpointRef = (new GlutenDriverEndpoint).self

  val executorDataMap = new ConcurrentHashMap[String, GlutenRegisterExecutor]
}
