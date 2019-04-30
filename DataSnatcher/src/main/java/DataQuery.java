import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.Gson;

import java.util.Scanner;

public class DataQuery {

  private static final String GET_URL = "http://www.numbeo.com:8008/api/";
  private static final String REQUEST_TYPE = "city_prices";
  private static final String API_KEY = "bmfcfv6t2ajeki";
  private static final String USER_AGENT = "Mozilla/5.0";

  private static List<City> citiesList;
  private static List<String> cityNames;


  public static String sendGET(String cityName, String originalName) throws IOException {
    String requestUrl = String
        .format("%s%s?api_key=%s&query=%s&currency=USD", GET_URL, REQUEST_TYPE, API_KEY, cityName);

    URL obj = new URL(requestUrl);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();

    con.setRequestMethod("GET");
    con.setRequestProperty("User-Agent", USER_AGENT);
    int responseCode = con.getResponseCode();

    System.out.println("GET Response Code :: " + responseCode);
    if (responseCode == HttpURLConnection.HTTP_OK) { // success
      BufferedReader in = new BufferedReader(new InputStreamReader(
          con.getInputStream()));
      String inputLine;
      StringBuffer response = new StringBuffer();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }

      convertStringToCity(response, originalName);
      in.close();
    } else {
      System.out.println("GET request failed for city: " + cityName);
    }

    return "";
  }

  public static void getCitiesList(String filename) {
    cityNames = new ArrayList<>();

    try {
      Scanner scan = new Scanner(new File(filename));
      while (scan.hasNextLine()) {
        String[] line = scan.nextLine().split(",");
        String name = line[0].trim();
        cityNames.add(name);
      }
      scan.close();
    } catch (IOException e) {
      System.err.println("File not found!");
    }

  }

  public static void convertStringToCity(StringBuffer response, String originalRequest) {
    Gson jsonParser = new Gson();
    City cityToAdd = jsonParser.fromJson(response.toString(), City.class);


    if (cityToAdd.name == null) {
      System.out.println(originalRequest);
    }

    cityToAdd.name = originalRequest; // Change name back to original request
    citiesList.add(cityToAdd);
  }

  public static void writeCitiesToJSON(String filename) {
    Gson jsonParser = new Gson();
    File outputFile = new File(filename);

    try {
      PrintWriter out = new PrintWriter(outputFile);
      for (City city: citiesList){
        out.write(jsonParser.toJson(city));
        out.write("\n");
      }
      out.close();
    } catch (IOException e) {
      System.err.println("Problem with writing to file " + filename);
    }
  }

  public static void writeCitiesToCSV(String filename) {
    File outputFile = new File(filename);
    try {
      PrintWriter out = new PrintWriter(outputFile);

      String key = getCSVKey();
      out.write(key + "\n");

      for (City city: citiesList) {
        out.write(city.cityCSV());
        out.write("\n");
      }

      out.close();
    } catch (IOException e) {
      System.err.println("Problem with writing to file " + filename);
    }
  }

  private static String getCSVKey() {
    StringBuilder header = new StringBuilder(";name;currency");
    for (int i = 1; i <= 121; i++) {
      header.append(";"+i);
    }
    return header.toString();
  }

  public static void main(String[] args) {

    ParseHtml.readHtml("http://insideairbnb.com/get-the-data.html");
    ParseHtml.extractLinks();
    ParseHtml.writeHtml("cities.csv");

    getCitiesList("cities.csv");

    citiesList = new ArrayList<>();

    System.out.println("=====  Starting requests... ======");

    // The specific cases are regions which are converted into cities within the regions.
    // Because you can't query Numbeo by region, I chose the largest city within that region to query by.
    // I also send along the original request, and change the response's name to match with AirBnB's data.
    try {
      //FileWriter fw = new FileWriter(new File("output.txt"));
      //BufferedWriter bw = new BufferedWriter(fw);
      for (String cityName: cityNames) {
        switch (cityName) {
          case "Tasmania": sendGET("Hobart", cityName); break; // Tasmania's capital
          case "Sicily"  : sendGET("Palermo", cityName); break; // Sicily's capital
          case "Barossa Valley" : sendGET("Adelaide", cityName); break; // Nearest city to Barossa Valley
          case "Barwon South West" : sendGET("Geelong", cityName); break; // Biggest city in Barwon South West Region
          case "Euskadi" : sendGET("Bilbao", cityName); break;
          case "Greater Manchester" : sendGET("Manchester", cityName); break;
          case "Menorca" : break; // Skip this one
          case "Northern Rivers" : sendGET("Brisbane", cityName); break; // Australia Region
          case "Puglia" : sendGET("Bari", cityName); break; // Italy
          case "Twin Cities MSA" : sendGET("Minneapolis", cityName); break;
          case "Vaud" : sendGET("Geneva", cityName); break; // Switzerland
          case "Western Australia" : sendGET("Perth", cityName); break; // Biggest city in Western Australia
          default: sendGET(cityName.replaceAll(" ", "%20"), cityName); break;
        }
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }

    System.out.println("=====  GET requests done  ======");

    /*
    System.out.println("=====  Writing to Json... ======");
    writeCitiesToJSON("city_prices.json");
    System.out.println("=====  Json writing done  ======");
    */
    System.out.println("=====  Writing to CSV...  ======");
    writeCitiesToCSV("city_prices.csv");
    System.out.println("=====  CSV writing done   ======");

  }


}
