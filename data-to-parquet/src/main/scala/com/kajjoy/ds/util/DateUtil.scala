package com.kajjoy.ds.util

import java.util.{Calendar, Date}

object DateUtil {

  /**
    * Provides the year
    * @return
    */
  def getYear() : String = {
    Calendar.getInstance().get(Calendar.YEAR).toString
  }

  /**
    * provides the month
    * @param date = input date defaults to current date
    * @return
    */
  def getMonth(date: Date = new Date()): String = {
    "%02d" format Calendar.getInstance().get(Calendar.MONTH) + 1
  }

  /**
    * provides the day of the month
    * @param date = input date defaults to current date
    * @return
    */
  def getDayOfTheMonth(date: Date = new Date()): String = {
    "%02d" format  Calendar.getInstance().get(Calendar.DAY_OF_MONTH)
  }


}
