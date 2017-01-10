package de.scdm.lambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazonaws.services.lambda.runtime.Context;

import de.scdm.elk.aws.S3Util;

public class WmDataParsingLambda implements
    com.amazonaws.services.lambda.runtime.RequestHandler<Void, String> {

  @Override
  public String handleRequest(Void arg0, Context arg1) {
    String returnMassage = null;

    try {
      // Get the current file from S3
      String key = "/2016-11-01/export_HISTORICAL_WM_DATA.csv";
      InputStream csvStream = S3Util.getInputStreamFromS3(key);
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(csvStream, "UTF-8"));

      for (String line = reader.readLine(); line != null; line = reader
          .readLine()) {
        System.out.println(line);
      }

      returnMassage = "SUCCESS";
    } catch (final Exception e) {
      returnMassage = "ERROR";
    }

    return returnMassage;
  }

}
