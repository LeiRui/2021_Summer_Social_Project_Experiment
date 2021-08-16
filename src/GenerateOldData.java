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

// 为了去除现有数据库中其它表格的影响，单独把测试对象表格DeviceRecord的数据写入到一个空的新数据库中
public class GenerateOldData {

  public static void main(String[] args) {
    InfluxDB influxDB = null;
    try {
      OkHttpClient.Builder client = new OkHttpClient.Builder();
      String serverURL = "http://127.0.0.1:8086", username = "root", password = "root";
      influxDB = InfluxDBFactory
          .connect(serverURL, username, password, client, ResponseFormat.MSGPACK);
      TimeUnit timeUnit = TimeUnit.MILLISECONDS; //timeUnit毫秒是考虑到孚创数据精度极限是100ms，所以不用纳秒
      String oldDatabaseName = "DataStream";

      // 创建一个新database
      String newDatabaseName = "testold";
      influxDB.query(new Query("CREATE DATABASE " + newDatabaseName));
      influxDB.setDatabase(newDatabaseName);
      // 在新数据库里创建一个新的默认retention policy（示例如下），否则自动默认是autogen
      /*
        String retentionPolicyName = "one_day_only";
        influxDB.query(new Query("CREATE RETENTION POLICY " + retentionPolicyName
            + " ON " + databaseName + " DURATION 1d REPLICATION 1 DEFAULT"));
        influxDB.setRetentionPolicy(retentionPolicyName);
      */
      // 可以用show retention policies来查看retention策略
      /*
        > show retention policies
        name    duration shardGroupDuration replicaN default
        ----    -------- ------------------ -------- -------
        autogen 0s       168h0m0s           1        true
      */
      String retentionPolicyName = "autogen"; // 默认策略

      // 允许批量写入来获得更好的性能
      influxDB.enableBatch(BatchOptions.DEFAULTS);

      long nanoCurrentTimestamp = System.currentTimeMillis() * 1000000; //ns
      long hourDuration = 3600000000000L; // 1小时
      String queryFormat = "SELECT time, Describe, GeneratorSetId, GeneratorSetNowDataReferenceId, "
          + "No, Type, Unit, Value FROM DeviceRecord where time > %d and time <= %d";
      // 往前推两年可以完全覆盖已有数据了
      for (int offset = 365 * 24 * 2; offset >= 1; offset--) {
        String query = String.format(queryFormat, nanoCurrentTimestamp - offset * hourDuration,
            nanoCurrentTimestamp - (offset - 1) * hourDuration);
        System.out.println(query);
        influxDB.setDatabase(oldDatabaseName);
        // 一定注意查询结果使用的精度要和写入数据使用的精度要保持一致。
        QueryResult queryResult = influxDB.query(new Query(query), timeUnit);
        List<Series> seriesList = queryResult.getResults().iterator().next().getSeries();
        if (seriesList == null) {
          continue;
        }
        List<List<Object>> values = seriesList.iterator().next().getValues();
        influxDB.setDatabase(newDatabaseName);
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

          influxDB.write(newDatabaseName, retentionPolicyName, Point
              .measurement("DeviceRecord")
              .time(timestamp, timeUnit) // 一定注意查询结果使用的精度要和写入数据使用的精度要保持一致。
              .tag("GeneratorSetId", GeneratorSetId)
              .tag("GeneratorSetNowDataReferenceId", GeneratorSetNowDataReferenceId)
              .tag("No", No)
              .tag("Type", Type)
              .addField("Describe", Describe)
              .addField("Unit", Unit)
              .addField("Value", Value) // Value项是string类型，和原数据库一致
              .build());
        }
      }
    } finally {
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
