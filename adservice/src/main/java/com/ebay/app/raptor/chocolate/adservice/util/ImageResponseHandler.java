package com.ebay.app.raptor.chocolate.adservice.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by jialili1 on 9/18/19
 *
 * Send 1x1 pixel response for impression event
 */
public class ImageResponseHandler {

  private static final Logger logger = LoggerFactory.getLogger(ImageResponseHandler.class);

  // 1x1 pixel.gif is a 42 byte file.
  private static volatile byte[] pixel = new byte[] { (byte) 0x47, (byte) 0x49, (byte) 0x46, (byte) 0x38, (byte) 0x39,
      (byte) 0x61, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x80, (byte) 0xff, (byte) 0x00,
      (byte) 0xc0, (byte) 0xc0, (byte) 0xc0, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x21, (byte) 0xf9,
      (byte) 0x04, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x2c, (byte) 0x00,
      (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x00,
      (byte) 0x01, (byte) 0x01, (byte) 0x32, (byte) 0x00, (byte) 0x3b };

  /**
   * Send a 1x1 pixel image back to the client
   *
   * @param response the HTTP servlet response
   */
  public static void sendImageResponse(final HttpServletResponse response) {
    OutputStream os = null;
    try {
      //	Set content headers and then write content to response
      response.setHeader("Cache-Control", "private, no-cache");
      response.setHeader("Pragma", "no-cache");
      response.setContentType("image/gif");
      response.setContentLength(pixel.length);
      response.setStatus(HttpServletResponse.SC_OK);

      os = response.getOutputStream();
      os.write(pixel);

    } catch (Exception e) {
      logger.warn("Failed to send pixel", e);
    } finally {
      closeOutputStream(os);
    }
  }

  private static void closeOutputStream(final OutputStream os) {
    if (os != null) {
      try {
        os.close();
      } catch (IOException e) {
        logger.warn("Failed to close output stream", e);
      }
    }
  }
}
