import java.util.List;
import java.util.Map;

public class City {

  public String name, currency;
  public Integer contributors, monthLastUpdate, yearLastUpdate;
  public Double latitude;
  public Double longitude;
  public Integer city_id;
  public List<PriceItem> prices;
  //public Map<String, PriceItem> prices;

  // For testing only
  public City (String name) {
//    this.latitude = latitude;
//    this.longitude = longitude;
//    this.city_id = city_id;
  }

  public PriceItem getPriceItemById(int item_id) {
    for (PriceItem pi: prices) {
      if (pi.item_id == item_id) {
        return pi;
      }
    }
    return null;
  }

  public String cityCSV() {
    StringBuilder sb = new StringBuilder(";"+name+";"+currency);
    for (int i = 1; i <= 121; i++) {
      PriceItem piForColumn = getPriceItemById(i);
      if (piForColumn != null) {
        sb.append(";"+piForColumn.toString());
      }
      else {
        sb.append(";");
      }
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return String.format("Name: %s, Currency: %s", name, currency);
  }

}
