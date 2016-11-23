package de.scdm.elk.rme;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class EmrParser {

  public static void main(final String[] args) throws ClassNotFoundException {

    final SparkConf sparkConf = new SparkConf().setAppName("JsonParser");

    final long start = new Date().getTime();

    final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",
        "AKIAINCDE4F7YE2WQQUQ");
    jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",
        "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");
    JavaRDD<String> textFile = jsc
        .textFile("s3n://wm-json-parser/hist_wm_data_nov-2016.csv");

    try {

      // Call the RDD
      final JavaRDD<String> lines = textFile
          .map(new Function<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(final String line) throws IOException {
              return new WmDataParsingService().call(line);
            }
          });

      // Write back JSON to S3
      lines.saveAsTextFile(
          "s3n://wm-json-parser/hist_wm_data_nov-2016_result.csv");

    } catch (final Exception e) {
      e.printStackTrace();
    } finally {
      final long end = new Date().getTime();
      final long spanInSeconds = (end - start) / 1000;
      System.out.println("This operation took " + spanInSeconds + "s\n");
      jsc.stop();
      jsc.close();
    }
  }

}
