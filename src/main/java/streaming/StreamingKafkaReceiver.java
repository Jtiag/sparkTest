package streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author jiangtao7
 * @date 2017/11/7
 */
public class StreamingKafkaReceiver {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("StreamingKafkaReceiver")
                .setMaster("local[2]")
                .set("spark.driver.allowMultipleContexts", "true");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        String zkQuorum = "hadoop01:2181,hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181";
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("pageXiaoChongPro", 1);
        JavaPairReceiverInputDStream<String, String> sources = KafkaUtils.createStream(jssc, zkQuorum, "kafka-test", topics);

        JavaDStream<JSONObject> devLogJavaDstream = sources.flatMap(new FlatMapFunction<Tuple2<String, String>, JSONObject>() {

            @Override
            public Iterator<JSONObject> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                JSONObject jsonObject = JSON.parseObject(stringStringTuple2._2);
                return Arrays.asList(jsonObject).iterator();
            }
        });
        devLogJavaDstream.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            @Override
            public void call(JavaRDD<JSONObject> jsonObjectJavaRDD) throws Exception {

                jsonObjectJavaRDD.foreachPartition(new VoidFunction<Iterator<JSONObject>>() {
                    @Override
                    public void call(Iterator<JSONObject> jsonObjectIterator) throws Exception {
                        DeviceLog deviceLog = new DeviceLog();
                        while (jsonObjectIterator.hasNext()) {
                            JSONObject jsonObject = jsonObjectIterator.next();
                            deviceLog.setAction(jsonObject.getString("action"));
                            deviceLog.setAppId(jsonObject.getString("appId"));
                            deviceLog.setCity(jsonObject.getString("city"));
                            deviceLog.setCountry(jsonObject.getString("country"));
                            deviceLog.setDeviceBrand(jsonObject.getString("deviceBrand"));
                            deviceLog.setDeviceModel(jsonObject.getString("deviceModel"));
                            deviceLog.setImei(jsonObject.getString("imei"));
                            deviceLog.setIp(jsonObject.getString("ip"));
                            deviceLog.setLang(jsonObject.getString("lang"));
                            deviceLog.setModuleId(jsonObject.getString("moduleId"));
                            deviceLog.setNettype(jsonObject.getString("nettype"));
                            deviceLog.setOsname(jsonObject.getString("osname"));
                            deviceLog.setOsVer(jsonObject.getString("osVer"));
                            deviceLog.setPageId(jsonObject.getString("pageId"));
                            deviceLog.setProvince(jsonObject.getString("province"));
                            deviceLog.setScreen(jsonObject.getString("screen"));
                            deviceLog.setStay_time(jsonObject.getLong("stay_time"));
                            deviceLog.setTimestamp(jsonObject.getString("timestamp"));

                            System.out.println(deviceLog);
                        }

                    }
                });
            }
        });
//        stringJavaDStream.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
//
//
//                stringJavaRDD.map(new Function<String, String>() {
//                    @Override
//                    public String call(String s) throws Exception {
//                        DeviceLog deviceLog = new DeviceLog();
//
////                        deviceLog.setAction(s.get);
//                        return null;
//                    }
//                });
//
//                SQLContext sqlContext = SQLContext.getOrCreate(stringJavaRDD.context());
//                Dataset<Row> dataFrame = sqlContext.createDataFrame(stringJavaRDD, DeviceLog.class);
//
//                dataFrame.registerTempTable("devLog");
//
//                Dataset<Row> sql = sqlContext.sql("select * from devLog");
//                sql.show();
//            }
//        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
