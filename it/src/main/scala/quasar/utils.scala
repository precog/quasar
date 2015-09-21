package quasar

import Predef._

package object utils {
  /**
   * Removes the extension from a file name
   * @param fileName File name with an extension
   * @return The name of the file without it's extension. Passing in "zips.data" would yield "zips".
   *         Passing in "zips.latest.data" would yield "zips.latest".
   */
  def removeExtension(fileName: String):String = {
    fileName.take(fileName.lastIndexOf('.'))
  }
}
