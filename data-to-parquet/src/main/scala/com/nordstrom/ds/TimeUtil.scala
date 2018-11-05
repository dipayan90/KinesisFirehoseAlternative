package com.nordstrom.ds
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Extract year, month, dat, hour from Date String with using Joda Date format.
  * @param timeIntervalFormat - JODA time format
  * @param timeInterval - date for conversion
  */
class TimeUtil(val timeIntervalFormat: String, val timeInterval: String) extends Serializable {
  val JODA_YEAR_FORMAT = "yyyy"
  val JODA_MONTH_FORMAT = "MM"
  val JODA_DAY_FORMAT = "dd"
  val JODA_HOUR_FORMAT = "HH"

  val formatter: DateTimeFormatter = DateTimeFormat.forPattern(timeIntervalFormat)
  val dt: DateTime = formatter.parseDateTime(timeInterval)

  def year: String = dt.toString(JODA_YEAR_FORMAT)

  def month: String = dt.toString(JODA_MONTH_FORMAT)

  def day: String = dt.toString(JODA_DAY_FORMAT)

  def hour: String = dt.toString(JODA_HOUR_FORMAT)

}

