import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Scanner;

public class ParseHtml {

  public static ArrayList<String> html = new ArrayList<>();
  public static ArrayList<String> cities = new ArrayList<>();

  public static void readHtml(String url) {
    try {
      Scanner in = new Scanner(new URL(url).openStream());

      while (in.hasNextLine()) {
        String line = in.nextLine().trim();
        if (!line.isEmpty()) {
          html.add(line);
        }
      }
      in.close();
    } catch (Exception e) {
      System.out.println(e);
      System.out.println("Cannot read " + url);
    }
  }

  public static void extractLinks() {
    for (String line : html) {
      if (line.contains("<h2>") && line.contains("</h2>")) {
        String link = line.substring(line.indexOf("<h2>")+4, line.lastIndexOf("</h2>"));
        cities.add(link);
      }
    }
  }

  public static void writeHtml(String filename) {
    try {
      PrintWriter writer = new PrintWriter(new File(filename));
      for (String city: cities) {
        writer.println(city);
      }
      writer.close();
    } catch (IOException e) {
      System.out.println("Could not write to file...");
    }
  }

}