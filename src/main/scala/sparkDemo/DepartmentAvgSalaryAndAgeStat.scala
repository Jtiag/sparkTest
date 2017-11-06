package sparkDemo

import org.apache.spark.sql.SparkSession

/**
  * Created by jiangtao7 on 2017/10/25.
  * 计算部门的平均薪资和年龄
  *
  * 需求：
  * 1、只统计年龄在20岁以上的员工
  * 2、根据部门名称和员工性别为粒度来进行统计
  * 3、统计出每个部门分性别的平均薪资和年龄
  *
  */
object DepartmentAvgSalaryAndAgeStat {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "hdfs://Amb:8020/tmp/spark-warehouse"
    val spark = SparkSession.builder()
      .appName("DepartmentAvgSalaryAndAgeStat")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    /*导入spark的隐式转换*/
    import spark.implicits._
    /*导入spark sql的functions*/
    import org.apache.spark.sql.functions._
    /*默认是从hdfs上读取文件*/
    val department = spark.read.json("/tmp/department.json");
    val employee = spark.read.json("/tmp/employee.json")

    employee.filter("age > 20")

      /**
        * 需要跟department数据进行join，然后才能根据部门名称和员工性别进行聚合
        * 注意：untyped join，两个表的字段的连接条件，需要使用三个等号
        */
      .join(department, $"depId" === $"id")
      .groupBy(department("name"), employee("gender"))
      .agg(avg(employee("salary")), avg(employee("age")))
      .show()
    /**
      * 基础的知识带一下
      * *
      * dataframe == dataset[Row]
      * dataframe的类型是Row，所以是untyped类型，弱类型
      * dataset的类型通常是我们自定义的case class，所以是typed类型，强类型
      * *
      * dataset开发，与rdd开发有很多的共同点
      * 比如说，dataset api也分成transformation和action，transformation是lazy特性的
      * action会触发实际的计算和操作
      * *
      * dataset也是有持久化的概念的
      */
  }
}
