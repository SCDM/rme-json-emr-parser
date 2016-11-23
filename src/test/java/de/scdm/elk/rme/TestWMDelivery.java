package de.scdm.elk.rme;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import de.scdm.elk.aws.S3Util;

public class TestWMDelivery {

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");

  private static S3ObjectSummary getCsvFile(List<S3ObjectSummary> allFiles) {
    for (final S3ObjectSummary objectSummary : allFiles) {
      if (objectSummary.getKey().toLowerCase().contains(".csv")) {
        return objectSummary;
      }
    }
    return null;
  }

  @Test
  public void test() throws ClassNotFoundException {

    final long start = new Date().getTime();
    try {
      // get all the keys
      final List<S3ObjectSummary> finalPricelist = S3Util
          .getObjectListOfFolder("hist_wm_data_nov-2016_test_3.csv");

      System.out.println("List size: " + finalPricelist.size());

      S3ObjectSummary finalPrice = null;

      if (finalPricelist.size() == 1) {
        finalPrice = getCsvFile(finalPricelist);
      }

      String finalPrices = null;
      List<String> finalPricesList = null;
      final Map<String, String> cleanupPriceMap = new HashMap<String, String>();

      if (finalPrice != null) {
        System.out.println("Selected csv file: " + finalPrice.getKey());
        finalPrices = new String(
            S3Util.getByteArrayFromS3(finalPrice.getKey()));
        finalPricesList = new LinkedList<String>(
            Arrays.asList(Pattern.compile("\n").split(finalPrices)));

        // Remove the header line
        finalPricesList.remove(0);

        // Split for the first comma to get the Scdm ID
        for (final String valueRow : finalPricesList) {
          final String[] field = valueRow.split(",");
          final String scdmId = field[0];
          if (!scdmId.isEmpty() && scdmId != null) {
            cleanupPriceMap.put(scdmId, valueRow);
          }
        }
        System.out.println(
            "Result in the price cleanup map: " + cleanupPriceMap.size());
      }

      // Call the RDD
      final List<String> priceList = new ArrayList<String>(
          cleanupPriceMap.values());
      final List<String> foundValuations = new ArrayList<String>();

      for (final String price : priceList) {
        foundValuations.add(new WmDataParsingService().call(price));
      }

      for (final String csvData : foundValuations) {
        System.out.println("Result:");
        System.out.println(csvData);
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
