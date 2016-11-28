import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Locale

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    // Read the file with all events indexed
    val eventIndex = Source.fromFile("EventIndex").getLines.map(_.split(","))
      .map(x => x(1) -> x(0).toInt).toMap

    // Get and parse all events files then sort the events by timestamp
    val eventsFiles = getFiles(new File("L3P3/RawData"))
    val events = eventsFiles.flatMap(x => parseEvents(x)).sortBy(_._1)

    val eventFiltered = events.groupBy(_._2).filter(_._2.length >= 10).keys.toList
    // For each time in the events, look 5 previous minutes and 5 next minutes and
    // create the window with the events that occurs in those intervals
    val windows = events.flatMap(x => List(x._1, x._1 + 299, x._1 - 300, x._1 - 1))
      .distinct.sorted
      .map(x => (
        events.filter(y => y._1 > x - 300 && y._1 <= x),
        events.filter(y => y._1 > x && y._1 <= x + 300)))
      .map(x => (
        x._1.map(y => eventIndex(y._2)).distinct.sorted.map(_ + ":1").mkString(" "),
        x._2.map(y => eventIndex(y._2)).distinct))

    // Write the events in libsvm files
    eventIndex.filter(eventFiltered contains _._1).foreach(x => {
      val writer = new PrintWriter(new File("libsvm/" + x._1 + ".libsvm"))
      windows.map(y => (if (y._2.contains(x._2)) "1" else "0") + " " + y._1 + "\n")
        .foreach(writer.write)
      writer.close
    })
  }
  /**
   * Search recursively in the directory and subdirectories all files with the word
   * "Events" in their name
   *
   * @param file Root directory where the search starts
   * @return A list with all the files found
   */
  def getFiles(file: File): List[File] = {
    val parts = file.listFiles.toList.partition(_.isDirectory)
    parts._2.filter(_.getName.contains("Events")) ::: parts._1.flatMap(getFiles)
  }

  /**
   * Reads and parse a csv file.
   * It will try to read the file with "UTF-8" by default, if it fails it will
   * call itself with encoding "windows-1252"
   * The name of the event is composed joining the "Name" with the "Event Type"
   *
   * @param csvFile  CSV file to parse
   * @param encoding Encoding used to read the file
   * @return List with all events in the file and their timestamps in seconds
   */
  def parseEvents(csvFile: File, encoding: String = "UTF-8"): List[(Long, String)] = {
    val dateParser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss z", new Locale("es", "ES"))
    val dateParser2 = new SimpleDateFormat("MMM dd yyyy HH:mm:ss z")
    try {
      val src = Source.fromFile(csvFile, encoding)
      val header = src.getLines.next.split(",")
      val dateIndex = header indexOf "Created On"
      val nameIndex = header indexOf "Name"
      val codeIndex = header indexOf "Event Type"
      src.getLines.map(_.split(",")).map(x => {
        if (x(dateIndex).matches("^[0-9].*"))
          (dateParser.parse(x(dateIndex)).getTime / 1000, x(nameIndex) + "-" + x(codeIndex))
        else
          (dateParser2.parse(x(dateIndex)).getTime / 1000, x(nameIndex) + "-" + x(codeIndex))
      }).toList
    } catch {
      case ex:
        Exception =>
        if (encoding != "UTF-8") throw ex
        else parseEvents(csvFile, "windows-1252")
    }
  }
}
