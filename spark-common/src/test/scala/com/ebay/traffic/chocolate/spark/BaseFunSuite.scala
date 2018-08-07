package com.ebay.traffic.chocolate.spark

import java.io.{File, IOException}
import java.net.{URL, URLClassLoader}
import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by yliu29 on 11/3/17.
  *
  * Base abstract class for all unit tests in Chocolate for handling common functionality.
  */
abstract class BaseFunSuite extends FunSuite with BeforeAndAfterAll {

  /**
    * Get test resource file
    *
    * @param file the file path in the resource
    * @return java.io.File of the test resource file
    */
  final def getTestResourceFile(file: String): File = {
    new File(getClass.getClassLoader.getResource(file).getFile)
  }

  /**
    * Get test resource file
    *
    * @param file the file path in the resource
    * @return the path string of the test resource file
    */
  final def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  /**
    * Create a temp directory for test
    * The temp directory will be deleted when test finished.
    *
    * The temp directory is a random directory, e.g. chocolate-ad1sdxzcvasdfxx
    *
    * @param root       the root path for the temp directory
    * @param namePrefix the prefix for the random temp file
    * @return java.io.File of the temp directory
    */
  final def createTempDir(root: String = System.getProperty("java.io.tmpdir"),
                          namePrefix: String = "chocolate"): File = {
    val dir = createDirectory(root, namePrefix)
    sys.addShutdownHook(deleteRecursively(dir))
    dir
  }

  /**
    * Create a temp directory for test
    * The temp directory will be deleted when test finished.
    *
    * The temp directory is a random directory, e.g. chocolate-ad1sdxzcvasdfxx
    *
    * @param root       the root path for the temp directory
    * @param namePrefix the prefix for the random temp file
    * @return java.io.File of the temp directory
    */
  final def createTempPath(root: String = System.getProperty("java.io.tmpdir"),
                           namePrefix: String = "chocolate"): String = {
    createTempDir(root, namePrefix).getCanonicalPath
  }

  private def createDirectory(root: String, namePrefix: String = "chocolate"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }

    dir.getCanonicalFile
  }

  private def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- file.listFiles()) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
    * Add file or directory to classpath
    *
    * @param file the file or directory to add to classpath
    */
  def addFileToClassLoader(file: File) = {
    addURLToClassLoader(file.toURI.toURL)
  }

  private def addURLToClassLoader(url: URL) = {
    val classLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
    val clazz = classOf[URLClassLoader]
    val method = clazz.getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(classLoader, url)
  }
}