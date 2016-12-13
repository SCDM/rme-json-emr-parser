package de.scdm.elk.rme;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import akka.dispatch.Foreach;
import de.scdm.elk.aws.S3Util;

public class TestWMDelivery {

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");


  @Test
  public void test() throws ClassNotFoundException {

    final long start = new Date().getTime();
    try {
      //String readFileToString = FileUtils.readFileToString(new File("/Users/hubertusbecker/Downloads/export_HISTORICAL_WM_DATA.csv"));
      List<String> readLines = FileUtils.readLines(new File("/Users/hubertusbecker/Downloads/export_HISTORICAL_WM_DATA.csv"));
      for (String string : readLines) {
        //System.out.println(string);
        String call = new WmDataParsingService().call(string);
        System.out.println(call);
      }

    } catch (final Exception e) {
      e.printStackTrace();
    } finally {
      final long end = new Date().getTime();
      final long spanInSeconds = (end - start) / 1000;
      System.out.println("This operation took " + spanInSeconds + "s\n");
    }
  }
}
