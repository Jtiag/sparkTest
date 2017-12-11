package sparkhnase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
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
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * @author jiangtao7
 * @date 2017/11/16
 */
public class SparkProcessHbaseData {
    private final static String hbase300Ip = "10.112.101.152:2181,10.112.101.151:2181,10.112.101.150:2181";
    private final static String hbase800Ip = "10.112.73.29:2181,10.112.73.30:2181,10.112.73.31:2181";
    private final static String znode300Parent = "/hbase-unsecure";
    private final static String znode800Parent = "/hbase";
    private static Log log = LogFactory.getLog(SparkProcessHbaseData.class);

    public static void main(String[] args) {
        log.info("app start......." + System.currentTimeMillis());

        SparkSession spark = SparkSession.builder().appName("SparkProcessHbaseData").master("local[*]").getOrCreate();
        /**
         * 写hbase
         */
//        writeDataToHbase(spark);
        /**
         * 读Hbase 并做简单的统计
         */
        readDataFromHbase(spark);

        log.info("app stop......." + System.currentTimeMillis());
    }

    private static void readDataFromHbase(SparkSession spark) {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = null;
        String tableName = "gome_sale";
        SparkContext sc = spark.sparkContext();
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        configuration.set(HConstants.ZOOKEEPER_QUORUM, hbase300Ip);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znode300Parent);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin hAdmin = connection.getAdmin();
            if (!hAdmin.tableExists(TableName.valueOf(tableName))) {
                System.out.println(tableName + " is not exist");
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("values"));
//        scan.addColumn(Bytes.toBytes("values"),Bytes.toBytes("uid"));
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = javaSparkContext.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//        List<Tuple2<ImmutableBytesWritable, Result>> tuple2List = javaPairRDD.take(100);
//        JavaRDD<Result> resultJavaRDD = javaPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Result>() {
//            @Override
//            public Result call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                return immutableBytesWritableResultTuple2._2;
//            }
//        });
        JavaRDD<Result> resultJavaRDD = javaPairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, Result>() {
            @Override
            public Iterator<Result> call(Iterator<Tuple2<ImmutableBytesWritable, Result>> tuple2Iterator) throws Exception {
                List<Result> results = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<ImmutableBytesWritable, Result> next = tuple2Iterator.next();
                    results.add(next._2);
                }
                return results.iterator();
            }
        });
        long count = resultJavaRDD.count();
        System.out.println(count);

//        try {
//            ClientProtos.Scan toScan = ProtobufUtil.toScan(scan);
//            String scanToString = Base64.encodeBytes(toScan.toByteArray());
//            configuration.set(TableInputFormat.SCAN, scanToString);
//            /**
//             * 返回值为tupe<主键，(列簇，列名，值)>）
//             */
//            JavaRDD<Schema> mapJavaRDD = resultJavaRDD.mapPartitions(new FlatMapFunction<Iterator<Result>, Schema>() {
//                @Override
//                public Iterator<Schema> call(Iterator<Result> resultIterator) throws Exception {
//                    List<Schema> schemaList = new ArrayList<>();
//                    String row;
//                    String cf;
//                    String col;
//                    String value;
//
//                    while (resultIterator.hasNext()) {
//                        Result result = resultIterator.next();
//                        Cell[] cells = result.rawCells();
//                        for (Cell cell : cells) {
//                            Schema schema = new Schema();
//                            row = new String(CellUtil.cloneRow(cell));
//                            cf = new String(CellUtil.cloneFamily(cell));
//                            col = new String(CellUtil.cloneQualifier(cell));
//                            value = new String(CellUtil.cloneValue(cell));
//                            schema.setRow(row);
//                            schema.setCf(cf);
//                            schema.setCname(col);
//                            schema.setValue(value);
//                            schemaList.add(schema);
//                        }
//                    }
//                    return schemaList.iterator();
//                }
//            });
//            Dataset<Row> dataFrame = spark.createDataFrame(mapJavaRDD, Schema.class);
//            dataFrame.createOrReplaceTempView("schema");
//            Dataset<Row> sql = spark.sql("select * from schema");
//            sql.show();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * 写hbase 操作麻烦 后面替换为 saveAsHbase
     *
     * @param spark
     */

    public static void writeDataToHbase(SparkSession spark) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
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
