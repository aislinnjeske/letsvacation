public class PriceItem {

  public Integer item_id, data_points, cpi_factor, rent_factor;
  public Double lowest_price, average_price, highest_price;
  public String item_name, category;

  @Override
  public String toString() {
    return String.format("%d %d %.2f %.2f %.2f %s", item_id, data_points, lowest_price, average_price, highest_price, item_name);
  }

}
