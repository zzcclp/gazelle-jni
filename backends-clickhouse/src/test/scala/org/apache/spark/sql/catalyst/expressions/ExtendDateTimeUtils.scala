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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._

import java.sql.Date
import java.time.ZoneId
import java.util.{Calendar, Locale, TimeZone}

object ExtendDateTimeUtils {

  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  final val MONTHS_PER_QUARTER: Long = 3L
  final val QUARTERS_PER_YEAR: Long = 4L

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  // TODO fixme spark3 millisToDaysLegacy
  // millisToDays() and fromJavaDate() are taken from Spark 2.4
  def millisToDaysLegacy(millisUtc: Long, timeZone: TimeZone): Int = {
    val millisLocal = millisUtc + timeZone.getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
  }

  def fromJavaDateLegacy(date: Date): Int = {
    millisToDaysLegacy(date.getTime, TimeZone.getTimeZone(ZoneId.systemDefault()))
  }

  // reverse of millisToDays
  def daysToMillis(days: SQLDate): Long = {
    daysToMillis(days, defaultTimeZone())
  }

  def daysToMillis(days: SQLDate, timeZone: TimeZone): Long = {
    val millisLocal = days.toLong * MILLIS_PER_DAY
    millisLocal - getOffsetFromLocalMillis(millisLocal, timeZone)
  }

  private[sql] def getOffsetFromLocalMillis(millisLocal: Long, tz: TimeZone): Long = {
    var guess = tz.getRawOffset
    // the actual offset should be calculated based on milliseconds in UTC
    val offset = tz.getOffset(millisLocal - guess)
    if (offset != guess) {
      guess = tz.getOffset(millisLocal - offset)
      if (guess != offset) {
        // fallback to do the reverse lookup using java.sql.Timestamp
        // this should only happen near the start or end of DST
        val days = Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
        val year = getYear(days)
        val month = getMonth(days)
        val day = getDayOfMonth(days)

        var millisOfDay = (millisLocal % MILLIS_PER_DAY).toInt
        if (millisOfDay < 0) {
          millisOfDay += MILLIS_PER_DAY.toInt
        }
        val seconds = (millisOfDay / 1000L).toInt
        val hh = seconds / 3600
        val mm = seconds / 60 % 60
        val ss = seconds % 60
        val ms = millisOfDay % 1000
        val calendar = Calendar.getInstance(tz, Locale.getDefault(Locale.Category.FORMAT))
        calendar.set(year, month - 1, day, hh, mm, ss)
        calendar.set(Calendar.MILLISECOND, ms)
        guess = (millisLocal - calendar.getTimeInMillis()).toInt
      }
    }
    guess
  }

  def floorDiv(x: Long, y: Long): Long = {
    var r = x / y
    if ((x ^ y) < 0L && r * y != x) r -= 1
    r
  }

  def floorMod(x: Long, y: Long): Long = x - floorDiv(x, y) * y

  private def lastDay(y: Int, m: Int) = m match {
    case 2 =>
      if (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) 29
      else 28
    case 4 => 30
    case 6 => 30
    case 9 => 30
    case 11 => 30
    case _ => 31
  }

  /**
   * Returns the ceil date time from original date time and trunc level. Trunc level should be
   * generated using `parseTruncLevel()`, should be between 1 and 8
   */
  def ceilTimestamp(t: SQLTimestamp, level: Int, zoneId: ZoneId): SQLTimestamp = {
    val floorValue = truncTimestamp(t, level, zoneId)
    if (floorValue == t) {
      floorValue
    } else {
      // trunc, then add a increment, trunc again === ceil
      val increment = level match {
        case TRUNC_TO_YEAR => 366 * MICROS_PER_DAY
        case TRUNC_TO_QUARTER => 93 * MICROS_PER_DAY
        case TRUNC_TO_MONTH => 31 * MICROS_PER_DAY
        case TRUNC_TO_WEEK => 7 * MICROS_PER_DAY
        case TRUNC_TO_DAY => MICROS_PER_DAY
        case TRUNC_TO_HOUR => 3600 * MICROS_PER_SECOND
        case TRUNC_TO_MINUTE => 60 * MICROS_PER_SECOND
        case TRUNC_TO_SECOND => MICROS_PER_SECOND
        case _ =>
          // caller make sure that this should never be reached
          sys.error(s"Invalid trunc level: $level")
      }
      truncTimestamp(floorValue + increment, level, zoneId)
    }
  }
}
