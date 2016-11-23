package de.scdm.elk.rme;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class WmDataParsingService {

  public String call(final String inputLine) throws IOException {
    System.out.println("***************************");
    System.out.println(inputLine);

    if (inputLine.contains("{") && inputLine.contains("}")) {
      String stringWoJson = removeJson(inputLine);
      String stringWoPi = stringWoJson.replaceAll("-3.141592653589793", "\"\"");
      String stringWoPi2 = stringWoPi.replaceAll("-31415", "\"\"");
      String stringWoDate = stringWoPi2.replaceAll("01-JAN-19", "\"\"");

      System.out.println(stringWoDate);

      // Split string into fields for processing
      // final String[] field = stringWoDate
      // .split("(\".*?\"|[^\",\\s]+)(?=\\s*,|\\s*$)");
      // System.out.println(field.length);

      Pattern p = Pattern.compile("(\".*?\"|[^\",\\s]+)(?=\\s*,|\\s*$)");
      Matcher m = p.matcher(stringWoDate);

      List<String> stringList = new ArrayList<String>();
      while (m.find()) {
        // System.out.println("Found a " + m.group());
        stringList.add(m.group());
      }
      System.out.println(stringList.size());

      // Replace id
      String stringWoId = stringWoDate.replaceAll(stringList.get(0), "\"\"");
      System.out.println(stringWoId);

      // Replace Date Fields
      String stringWoDates = stringWoId
          .replaceAll(stringList.get(2), parseDate(stringList.get(2))) // aoD
          .replaceAll(stringList.get(9), parseDate(stringList.get(9))) // issue
          .replaceAll(stringList.get(19), parseDate(stringList.get(19))) // lrd
          .replaceAll(stringList.get(22), parseDate(stringList.get(22)))
          .replaceAll(stringList.get(24), parseDate(stringList.get(24)))
          .replaceAll(stringList.get(26), parseDate(stringList.get(26)))
          .replaceAll(stringList.get(28), parseDate(stringList.get(28)))
          .replaceAll(stringList.get(30), parseDate(stringList.get(30)))
          .replaceAll(stringList.get(32), parseDate(stringList.get(32)))
          .replaceAll(stringList.get(34), parseDate(stringList.get(34)))
          .replaceAll(stringList.get(36), parseDate(stringList.get(36)))
          .replaceAll(stringList.get(38), parseDate(stringList.get(38)))
          .replaceAll(stringList.get(40), parseDate(stringList.get(40)))
          .replaceAll(stringList.get(42), parseDate(stringList.get(42)))
          .replaceAll(stringList.get(44), parseDate(stringList.get(44)))
          .replaceAll(stringList.get(46), parseDate(stringList.get(46)))
          .replaceAll(stringList.get(52), parseDate(stringList.get(52)))
          .replaceAll(stringList.get(53), parseDate(stringList.get(53)))
          .replaceAll(stringList.get(60), parseDate(stringList.get(60)))
          .replaceAll(stringList.get(61), parseDate(stringList.get(61)))
          .replaceAll(stringList.get(68), parseDate(stringList.get(68)))
          .replaceAll(stringList.get(90), parseDate(stringList.get(90)));

      System.out.println("Result");
      System.out.println(stringWoDates);
      System.out.println("Result end");
      // as of date
      // .replaceAll(field[9], parseDate(field[9])) // Issue Date
      // .replaceAll(field[19], parseDate(field[19])) // Last Redemption Date
      // .replaceAll(field[22], parseDate(field[22])) // Mdy St Asset
      // .replaceAll(field[22], parseDate(field[24])) // Snp St Asset
      // .replaceAll(field[26], parseDate(field[26])) // Dbrs St Asset
      // .replaceAll(field[28], parseDate(field[28])) // Mdy St Iss
      // .replaceAll(field[30], parseDate(field[30])) // Ftc St Iss
      // .replaceAll(field[32], parseDate(field[32])) // Snp St Iss
      // .replaceAll(field[34], parseDate(field[34])) // Mdy Lt Ass
      // .replaceAll(field[36], parseDate(field[36])) // Ftc Lt Ass
      // .replaceAll(field[38], parseDate(field[38])) // Snp Lt Ass
      // .replaceAll(field[40], parseDate(field[40])) // Dbrs Lt Ass
      // .replaceAll(field[42], parseDate(field[42])) // Mdy Lt Iss
      // .replaceAll(field[44], parseDate(field[44])) // Ftc Lt Iss
      // .replaceAll(field[46], parseDate(field[46])) // Snp Lt Iss
      // .replaceAll(field[52], parseDate(field[52])) // Creditor Prolong
      // .replaceAll(field[53], parseDate(field[53])) // Debitor Prolong
      // .replaceAll(field[60], parseDate(field[60])) // Bgn Interest Pay
      // .replaceAll(field[61], parseDate(field[61])) // End interest Pay
      // .replaceAll(field[68], parseDate(field[68])) // First coupon
      // .replaceAll(field[90], parseDate(field[90])); // Ftc St Ass

      return stringWoDates;
    } else {
      return inputLine;
    }
  }

  public String removeJson(String inputLine) {
    if (inputLine.contains("{") && inputLine.contains("}")) {

      int startIndex = inputLine.indexOf("{");
      int endIndex = 0;

      int countMatches = StringUtils.countMatches(inputLine, "}");
      if (countMatches > 1) {
        List<Integer> positionList = new ArrayList<Integer>();
        char[] array = inputLine.toCharArray();
        for (int i = 0; i < array.length; i++) {
          if (array[i] == '}') {
            positionList.add(i);
          }
        }

        for (int position : positionList) {
          int check = position;
          int newCheck = check + 1;
          if (Character.valueOf(inputLine.charAt(newCheck)).equals(',')) {
            System.out.println("Komma found");
            endIndex = check;
            break;
          }
        }
      } else {
        endIndex = inputLine.indexOf("}");
      }

      int lastIndex = inputLine.length();
      System.out.println("Start: " + startIndex);
      System.out.println("End: " + endIndex);

      StringBuilder sb = new StringBuilder();
      sb.append(inputLine.substring(0, startIndex));
      sb.append("\"\"");
      sb.append(inputLine.substring(endIndex + 1, lastIndex));
      return sb.toString();
    } else {
      return "\"\"";
    }
  }

  public String parseDate(String dateString) {
    if (!dateString.equals("\"\"")) {
      DateTimeFormatter dtf = DateTimeFormat.forPattern("dd-MMM-yy");
      DateTime jodaTime = dtf.parseDateTime(dateString);
      DateTimeFormatter dtfOut = DateTimeFormat.forPattern("dd-MMM-yyyy");
      String resultDate = dtfOut.print(jodaTime);

      return resultDate;
    } else {
      return "\"\"";
    }
  }
}
