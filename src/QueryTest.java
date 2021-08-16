import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class QueryTest {
  public static void main(String[] args) {
    InfluxDB influxDB = null;
    PrintWriter printWriter = null;
    try {
      OkHttpClient.Builder client = new OkHttpClient.Builder();
      String serverURL = "http://127.0.0.1:8086", username = "root", password = "root";
      influxDB = InfluxDBFactory
          .connect(serverURL, username, password, client, ResponseFormat.MSGPACK);

      String databaseName = "testnew";
      influxDB.setDatabase(databaseName);

      String csvFilePath = "D://" + databaseName + ".csv";
      printWriter = new PrintWriter(new FileWriter(csvFilePath));
      printWriter.println("GeneratorSetId,timeElapsed(ms)");

      String queryFormat = "SELECT * FROM DeviceRecord where GeneratorSetId = '%d' limit 40000";
      //
      List<Integer> GeneratorSetIdList = new ArrayList<>(
          Arrays.asList(1, 850, 1357, 1360, 1366, 1374, 1379, 1380, 1381, 1382, 1385, 1386, 1387,
              1390, 1545, 1551, 1556, 1567, 1568, 1570, 1637, 1638, 1639, 1640, 2095));
      for (Integer GeneratorSetId : GeneratorSetIdList) {
        printWriter.print(GeneratorSetId + ",");
        String sql = String.format(queryFormat, GeneratorSetId);
        System.out.println(sql);
        long startTime = System.nanoTime();
        QueryResult queryResult = influxDB.query(new Query(sql));
        long finishTime = System.nanoTime();
        long timeElapsed = finishTime - startTime;
        // 这一行是为了避免queryResult没有被使用而被优化掉不执行了。
        System.out.println(
            queryResult.getResults().iterator().next().getSeries().iterator().next().getValues()
                .size());
        System.out.println("time(ms): " + timeElapsed / 1000000.0);
        printWriter.print(timeElapsed / 1000000.0 + "\n");
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    } finally {
      printWriter.close();
      if (influxDB != null) {
        try {
          influxDB.close();
        } catch (Exception e) {
          System.out.println("Couldn't close the influxDB connection");
        }
      }
    }
  }
}
