import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;

// 把老数据库A里的DeviceRecord的数据按照新的数据模式写入到一个新数据库B中
public class TransferTool {

  public static void main(String[] args) {
    InfluxDB influxDB_A = null;
    InfluxDB influxDB_B = null;
    try {
      // 设置数据迁移的起点
      OkHttpClient.Builder client_A = new OkHttpClient.Builder();
      String serverURL_A = "http://127.0.0.1:8086", username_A = "root", password_A = "root"; //TODO 使用前记得修改确认
      influxDB_A = InfluxDBFactory
          .connect(serverURL_A, username_A, password_A, client_A,
              ResponseFormat.MSGPACK);
      TimeUnit timeUnit = TimeUnit.MILLISECONDS; //timeUnit毫秒是考虑到孚创数据精度极限是100ms，所以可以不用纳秒
      String databaseName_A = "DataStream";
      influxDB_A.setDatabase(databaseName_A);

      // 设置数据迁移的终点
      OkHttpClient.Builder client_B = new OkHttpClient.Builder();
      String serverURL_B = "http://182.230.192.87:8086", username_B = "root", password_B = "root"; //TODO 使用前记得修改确认
      influxDB_B = InfluxDBFactory
          .connect(serverURL_B, username_B, password_B,
              client_B, ResponseFormat.MSGPACK);
      String databaseName_B = "DataStream";
      if (!influxDB_B.databaseExists(databaseName_B)) {
        throw new IOException(
            "先完成步骤4，即在机器B的InfluxDB上创建和原来机器A中一样的数据库”DataStream”和retention policy");
      }
      influxDB_B.setDatabase(databaseName_B);
      String retentionPolicyName_B = "autogen"; // TODO 使用前记得修改确认
      influxDB_B.enableBatch(BatchOptions.DEFAULTS); // 允许批量写入来获得更好的性能

      //开始从老数据库A里查询数据，按照新的数据模式往新数据库B里写数据
      long nanoCurrentTimestamp = System.currentTimeMillis() * 1000000; //ns
      long hourDuration = 3600000000000L; // 1小时
      String queryFormat = "SELECT time, Describe, GeneratorSetId, GeneratorSetNowDataReferenceId, "
          + "No, Type, Unit, Value FROM DeviceRecord where time > %d and time <= %d";
      // 往前推两年可以完全覆盖已有数据了
      for (int offset = 365 * 24 * 3; offset >= 1; offset--) {
        String query = String.format(queryFormat, nanoCurrentTimestamp - offset * hourDuration,
            nanoCurrentTimestamp - (offset - 1) * hourDuration);
        System.out.println(query);
        // 一定注意查询结果使用的精度要和写入数据使用的精度要保持一致。
        QueryResult queryResult = influxDB_A.query(new Query(query), timeUnit);
        List<Series> seriesList = queryResult.getResults().iterator().next().getSeries();
        if (seriesList == null) {
          continue;
        }
        List<List<Object>> values = seriesList.iterator().next().getValues();
        for (List<Object> row : values) {
          long timestamp = (long) row.get(0);
          String Describe = (String) row.get(1);
          String GeneratorSetId = (String) row.get(2);
          String GeneratorSetNowDataReferenceId = (String) row.get(3);
          String No = (String) row.get(4);
          No = No.replaceAll("^\"|\"$", ""); //去掉No里的双引号，以减少查询语句不必要的繁琐
          String Type = (String) row.get(5);
          String Unit = (String) row.get(6);
          String Value = (String) row.get(7);

          influxDB_B
              .write(databaseName_B, retentionPolicyName_B, Point
                  .measurement("DeviceRecord")
                  .time(timestamp, timeUnit) // 注意查询结果使用的精度要和写入数据使用的精度要保持一致。
                  .tag("GeneratorSetId", GeneratorSetId)
                  .tag("GeneratorSetNowDataReferenceId", GeneratorSetNowDataReferenceId)
                  .tag("Describe", Describe)
                  .tag("No", No)
                  .tag("Type", Type)
                  .tag("Unit", Unit)
                  .addField("Value", Value) // 注意Value项是string类型，和原数据库一致
                  .build());
        }
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    } finally {
      if (influxDB_A != null) {
        try {
          influxDB_A.close();
        } catch (Exception e) {
          System.out.println("Couldn't close the influxDB_A connection");
        }
      }
      if (influxDB_B != null) {
        try {
          influxDB_B.close();
        } catch (Exception e) {
          System.out.println("Couldn't close the influxDB_B connection");
        }
      }
    }
  }
}
