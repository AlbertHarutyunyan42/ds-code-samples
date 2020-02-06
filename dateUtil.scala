import org.joda.time.{DateTime, Days}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
object DateUtil {
  var pattern = "yyyy-MM-dd"
  var formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)

  //range will be yesterday - DAYS
  def parseDate(days: Int, goBack: Int = 1): (DateTime, DateTime) = {
    val from = today().minusDays(days).minusDays(goBack)
    val to = today().minusDays(goBack)
    (from, to)
  }

  def parseDate(startDate: String, endDate: String): (DateTime, DateTime) = {
    val from = formatter.parseDateTime(startDate)
    val to = formatter.parseDateTime(endDate)
    (from, to)
  }

  def parseDate(date: String): DateTime = {
    val dateParsed = formatter.parseDateTime(date)
    dateParsed
  }

  def daysArr(StartDate: String, EndDate: String): Int = {
    val dur = parseDate(StartDate, EndDate)
    val daysT = Days.daysBetween(dur._1.withTimeAtStartOfDay(), dur._2.withTimeAtStartOfDay()).getDays
    daysT
  }

  def daysArr(days: Int, goBack: Int = 1): Int = {
    val dur = parseDate(days, goBack)
    val daysT = Days.daysBetween(dur._1.withTimeAtStartOfDay(), dur._2.withTimeAtStartOfDay()).getDays
    daysT
  }

  def daysBetween(from: String, to: String): IndexedSeq[DateTime] = {
    val daysCount = daysArr(from, to)
    (0 to daysCount).map { d =>
      parseDate(from).plusDays(d)
    }
  }

  def today(): DateTime = {
    new DateTime().withTimeAtStartOfDay()
  }
}

import DateUtil._
