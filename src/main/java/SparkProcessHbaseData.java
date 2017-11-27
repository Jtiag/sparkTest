import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * @author jiangtao7
 * @date 2017/11/16
 */
public class SparkProcessHbaseData {
    private final static String hbase300Ip = "10.112.101.152:2181,10.112.101.151:2181,10.112.101.150:2181";
    private final static String hbase800Ip = "10.112.73.29:2181,10.112.73.30:2181,10.112.73.31:2181";
    private final static String znode300Parent = "/hbase-unsecure";
    private final static String znode800Parent = "/hbase";
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkProcessHbaseData")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        /**
         * 写hbase
         */
//        writeDataToHbase(sc);
        /**
         * 读Hbase 并做简单的统计
         */
        readDataFromHbase(sc);
        sc.close();


    }

    private static void readDataFromHbase(JavaSparkContext sc) {
        Configuration configuration = HBaseConfiguration.create();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("values"));
        scan.addColumn(Bytes.toBytes("values"),Bytes.toBytes("uid"));

        String tableName = "BLUETOOTH_DATA_GOME";

        configuration.set(TableInputFormat.INPUT_TABLE,tableName);
        configuration.set(HConstants.ZOOKEEPER_QUORUM, hbase800Ip);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znode800Parent);

        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = sc.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        long count = javaPairRDD.count();
        System.out.println(count);

        try {
            ClientProtos.Scan toScan = ProtobufUtil.toScan(scan);
            String encodeBytes = Base64.encodeBytes(toScan.toByteArray());
            configuration.set(TableInputFormat.SCAN,encodeBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写hbase 操作麻烦 后面替换为 saveAsHbase
     * @param sc
     */
    public static void writeDataToHbase(JavaSparkContext sc) {
        JavaRDD<String> stringJavaRDD = sc.textFile("C:\\Users\\Administrator\\Desktop\\BOOLTH_DATA.txt");
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, hbase300Ip);
        hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");

        JobConf jobConf = new JobConf(hbaseConf, SparkProcessHbaseData.class);
        String[] cols = {"f1"};
        try {
            HbaseUtil.createTable("blue_tooth", cols);
        } catch (IOException e) {
            e.printStackTrace();
        }
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "blue_tooth");

        try {
            Job job = Job.getInstance(jobConf);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Result.class);
            job.setOutputFormatClass(TableOutputFormat.class);
            /**
             row Name:uid
             value:ff8080815bfb4629015c0ac67f481e3e
             RowName:yayyms-20170524-2002-111700-iCcurF6349058C
             Timetamp:1496653742828
             column Family:values
             row Name:value
             value:34.51
             */
            JavaPairRDD<ImmutableBytesWritable, Put> javaPairRDD = stringJavaRDD.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
                @Override
                public Tuple2<ImmutableBytesWritable, Put> call(String s) throws Exception {

                    String[] split = s.split(" ");
                    if (split.length == 2) {
                        String rowKey = split[0];
                        String info = split[1];

                        Put put = new Put(Bytes.toBytes(rowKey));

                        Put column = put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("info"), Bytes.toBytes(info));
                        return new Tuple2<>(new ImmutableBytesWritable(), column);
                    }
                    return null;
                }
            });
            javaPairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
