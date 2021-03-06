
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
package com.qubole.sparklens.analyzer

import java.util.concurrent.TimeUnit

import com.qubole.sparklens.scheduler.CompletionEstimator
import com.qubole.sparklens.common.{AggregateMetrics, AppContext}
import com.qubole.sparklens.timespan.ExecutorTimeSpan

import scala.collection.mutable

import scala.util.control.Breaks._

/*
 * Created by rohitk on 21/09/17.
 */
class ExecutorWallclockAnalyzer extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    val coresPerExecutor    =  ac.executorMap.values.map(x => x.cores).sum/ac.executorMap.size
    val appExecutorCount    =  ac.executorMap.size
    val testPercentages     =  Array(10, 20, 50, 80, 100, 110, 120, 150, 200, 300, 400, 500)

    out.println ("\n App completion time and cluster utilization estimates with different executor counts")
    val appRealDuration = endTime - startTime
    printModelError(ac, appRealDuration, out)


    val pool = java.util.concurrent.Executors.newFixedThreadPool(testPercentages.size)
    val results = new mutable.HashMap[Int, String]()
    var optimizedEstimatedTime = 0.toLong
    var optimizedEstimatedTimeInSec = 0.toLong
    var optimizedExecutorCount = 0
    for (percent <- testPercentages) {
      pool.execute( new Runnable {
        override def run(): Unit = {
          val executorCount = (appExecutorCount * percent)/100
          if (executorCount > 0) {
            val estimatedTime = CompletionEstimator.estimateAppWallClockTime(ac, executorCount, coresPerExecutor, appRealDuration)
            val estimatedTimeInSec = TimeUnit.MILLISECONDS.toSeconds(estimatedTime)
            if (optimizedEstimatedTimeInSec == 0 || estimatedTimeInSec < optimizedEstimatedTimeInSec) {
              optimizedExecutorCount = executorCount
              optimizedEstimatedTime = estimatedTime
              optimizedEstimatedTimeInSec = estimatedTimeInSec
            }
            val executorRuntime = ac.stageMap.map(x => x._2.stageMetrics.map(AggregateMetrics.executorRuntime).value).sum
//            out.println(s"runtime is ${executorRuntime} (${pd(executorRuntime)}) while estimated time is ${estimatedTime} (${pd(estimatedTime)})")
            val utilization =  ac.stageMap.map(x => x._2.stageMetrics.map(AggregateMetrics.executorRuntime).value).sum.toDouble*100/(estimatedTime*executorCount*coresPerExecutor)
            results.synchronized {
              results(percent) = f" Executor count ${executorCount}%5s  ($percent%3s%%) estimated time ${pd(estimatedTime)} and estimated cluster utilization ${utilization}%3.2f%%"
            }
          }
        }
      })
    }
    pool.shutdown()
    if (!pool.awaitTermination(2, TimeUnit.MINUTES)) {
      //we timed out
      out.println (
        s"""
           |WARN: Timed out calculating estimations for various executor counts.
           |WARN: ${results.size} of total ${testPercentages.size} estimates available at this time.
           |WARN: Please share the event log file with Qubole, to help us debug this further.
           |WARN: Apologies for the inconvenience.\n
         """.stripMargin)

    }
    //take a lock to prevent any conflicts while we are printing
    results.synchronized {
      results.toBuffer.sortWith((a, b) => a._1 < b._1)
        .foreach(x => {
          out.println(x._2)
        })
    }
    out.println(s"${Console.GREEN} The optimized number of executors is ${optimizedExecutorCount} with estimated time ${pd(optimizedEstimatedTime)}${Console.RESET}.\n")
    out.println("\n")

    analyzeBasedOnCore(ac, out, optimizedExecutorCount, optimizedEstimatedTime)
    out.toString()
  }

  def printModelError(ac: AppContext, appRealDuration: Long, out: mutable.StringBuilder): Unit = {
    val coresPerExecutor    =  ac.executorMap.values.map(x => x.cores).sum/ac.executorMap.size
    val appExecutorCount    =  ac.executorMap.size
    @volatile var estimatedTime: Long = -1
    val thread = new Thread {
      override def run(): Unit = {
        estimatedTime = CompletionEstimator.estimateAppWallClockTime(ac, appExecutorCount, coresPerExecutor, appRealDuration)
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread.join(60*1000)

    if (estimatedTime < 0) {
      //we timed out
      out.println (
        s"""
           |WARN: Timed out calculating model estimation time.
           |WARN: Please share the event log file with Qubole, to help us debug this further.
           |WARN: Apologies for the inconvenience.
         """.stripMargin)
      return
    }

    out.println (
      s"""
         | Real App Duration ${pd(appRealDuration)}
         | Model Estimation  ${pd(estimatedTime)}
         | Model Error       ${(Math.abs(appRealDuration-estimatedTime)*100)/appRealDuration}%
         |
         | NOTE: 1) Model error could be large when auto-scaling is enabled.
         |       2) Model doesn't handles multiple jobs run via thread-pool. For better insights into
         |          application scalability, please try such jobs one by one without thread-pool.
         |
       """.stripMargin)
  }

  def analyzeBasedOnCore(ac: AppContext, out: mutable.StringBuilder, optimizedExecutorCount: Int,
                         optimizedEstimatedTime: Long): Unit = {
    out.println("\n")
    val m4largeMap: mutable.Map[String, String] = mutable.HashMap("name" -> "m4large", "core" -> "2", "memory" -> "8", "cost" -> "0.1")
    val m4xlargeMap: mutable.Map[String, String] = mutable.HashMap("name" -> "m4xlarge", "core" -> "4", "memory" -> "16", "cost" -> "0.2")
    val m42xlargeMap: mutable.Map[String, String] = mutable.HashMap("name" -> "m42xlarge", "core" -> "8", "memory" -> "32", "cost" -> "0.4")
    val m44xlargeMap: mutable.Map[String, String] = mutable.HashMap("name" -> "m44xlarge", "core" -> "16", "memory" -> "64", "cost" -> "0.8")
    val m410xlargeMap: mutable.Map[String, String] = mutable.HashMap("name" -> "m410xlarge", "core" -> "40", "memory" -> "160", "cost" -> "2")
    val m416xlargeMap: mutable.Map[String, String] = mutable.HashMap("name" -> "m416xlarge", "core" -> "64", "memory" -> "256", "cost" -> "3.2")
    val instanceTypes: List[mutable.Map[String, String]] = List(m4largeMap, m4xlargeMap, m42xlargeMap, m44xlargeMap, m410xlargeMap, m416xlargeMap)

    ac.stageMap.foreach(x => out.println(" The number of task at stageID " + x._2.stageID + " is " + x._2.taskExecutionTimes.length))
    var maxTaskExecutionTimes = 0
    ac.stageMap.
      foreach( x => {
        if (maxTaskExecutionTimes < x._2.taskExecutionTimes.length) {
          maxTaskExecutionTimes = x._2.taskExecutionTimes.length
        }
      })
    out.println(" The maximum number of tasks at any stage is " + maxTaskExecutionTimes)
    val requiredCoresPerInstance = Math.ceil(maxTaskExecutionTimes/optimizedExecutorCount).toInt
    out.println(" The number of required cores per instance is " + requiredCoresPerInstance)

    /*out.println(" The number of cores per executor is " + coresPerExecutor)
    val requiredExecutors = Math.ceil(maxTaskExecutionTimes/coresPerExecutor).toInt
    out.println(" The number of executors required to run all tasks in parallel is " + requiredExecutors + " (max # of tasks / cores per execution)")
    val totalCores = ac.executorMap.values.map(x => x.cores).sum
    out.println(" The total number of required cores are " + totalCores)*/

    //Memory Analysis
    /*ac.stageMap.foreach(x => out.println(" \n The peak execution memory at stageID " + x._2.stageID + " is " + x._2.stageMetrics.map(AggregateMetrics.peakExecutionMemory).value))
    var maxPeakExecutionMemory = 0.toLong
    ac.stageMap.
      foreach( x => {
        val peakExecutionMemoryAtStage = x._2.stageMetrics.map(AggregateMetrics.peakExecutionMemory).value

        if (maxPeakExecutionMemory < peakExecutionMemoryAtStage) {
          maxPeakExecutionMemory =  peakExecutionMemoryAtStage
        }
      })

    if (maxPeakExecutionMemory != 0) {
      maxPeakExecutionMemory / (1024)
    }
    out.println(" The maximum peak execution memory at any stage is " + maxPeakExecutionMemory + "MB")*/

    var executorsPerInstance = 0
    instanceTypes.foreach ( instanceType => {
      val instanceName = instanceType("name")
      out.println("\n ***** " + instanceName + " ANALYSIS *****")
      /*val numInstance = Math.ceil(totalCores/instanceType("core").toInt).toInt
      instanceType += "numInstance" -> numInstance.toString
      out.println(" The number of " + instanceName + " instances: " + numInstance)*/
      //      out.println(" The optimizedEstimatedTime in second: " + TimeUnit.MILLISECONDS.toSeconds(optimizedEstimatedTime))
      val totalCost = TimeUnit.MILLISECONDS.toSeconds(optimizedEstimatedTime) * instanceType("cost").toDouble/3600 * optimizedExecutorCount
      out.println(" The total cost will be $" +  totalCost)
      instanceType += "totalCost" -> totalCost.toString

      /*val executorsPerInstanceBasedOnCore = (instanceType("core").toInt / coresPerExecutor)
      out.println(" The number of executors to fit in one " + instanceName + " instance based on core is " + executorsPerInstanceBasedOnCore)*/


      /*if (maxPeakExecutionMemory != 0) {
        val executorsPerInstanceBasedOnMemory = instanceType("memory").toLong*1024 / maxPeakExecutionMemory
        out.println(" The number of executors to fit in one " + instanceName + " instance based on memory is " + executorsPerInstanceBasedOnMemory)
        if (executorsPerInstanceBasedOnCore < executorsPerInstanceBasedOnMemory) {
          executorsPerInstance = executorsPerInstanceBasedOnCore
        } else {
          executorsPerInstance = executorsPerInstanceBasedOnMemory.toInt
        }
      }*/

      /*executorsPerInstance = executorsPerInstanceBasedOnCore
      if (executorsPerInstance == 0 ) {
        out.println(" WARNING: Not a single executor would fit in one " + instanceName + " instance!!")
      } else {
        val requiredInstance = Math.ceil(requiredExecutors / executorsPerInstance).toInt
        out.println(" The minimum number of " + instanceName + " instances to run al tasks in parallel is " + requiredInstance + " (# of required executors / # of executors per instance)")
      }*/

    })

    /*var leastTotalCost = 0.toDouble
    var leastCostInstanceType = ""
    instanceTypes.foreach ( instanceType => {
      if (leastTotalCost == 0 || instanceType("totalCost").toDouble < leastTotalCost) {
        leastTotalCost = instanceType("totalCost").toDouble
        leastCostInstanceType = instanceType("name")
//        leastNumInstance = instanceType("numInstance")
      }
    })*/

    var optimizedInstanceType = ""
    var optimizedTotalCost = 0.toDouble
    instanceTypes.foreach ( instanceType => {
      if (optimizedInstanceType.equalsIgnoreCase("") && instanceType("core").toInt >= requiredCoresPerInstance) {
        optimizedInstanceType = instanceType("name")
        optimizedTotalCost = instanceType("totalCost").toDouble
      }
    })
    out.println(s"\n${Console.GREEN} The total optimized cost will be $$"
      + BigDecimal(optimizedTotalCost).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      + " with " + optimizedExecutorCount + " " + optimizedInstanceType + s" instances${Console.RESET}")
  }
}
