package de.scdm.elk.rme;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Test;

import de.scdm.elk.aws.S3Util;

public class TestWmDeliveryS3 {

  @Test
  public void test() throws IOException {

    String key = "/2016-11-01/export_HISTORICAL_WM_DATA.csv";
    InputStream csvStream = S3Util.getInputStreamFromS3(key);
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(csvStream, "UTF-8"));

    for (String line = reader.readLine(); line != null; line = reader
        .readLine()) {
      System.out.println(line);
    }
  }

}
