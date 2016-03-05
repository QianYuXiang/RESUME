package com.meituan.shangchao

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

/**
  * Created by qianyuxiang on 16/1/29.
  */
object wmpeisongpaweather {
  def date2datekey(date: String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val format2 = new java.text.SimpleDateFormat("yyyyMMdd")
    val d = format.parse(date)
    format2.format(d)
  }

  def datekey2date(datekey: String) = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val format2 = new java.text.SimpleDateFormat("yyyyMMdd")
    val d = format2.parse(datekey)
    format.format(d)
  }

  def timestamp(date: String) = {
    Timestamp.valueOf(date).getTime()/1000
  }

  case class Weather(city_id: Long, city_name: String, weather_date: String, current_weather: String, timestamp: Long, dt: String) extends Ordered[Weather] {
    def compare(that: Weather) = (this.timestamp - that.timestamp).asInstanceOf[Int]
  }

  def what_weather(ts: Long, weatherlist: Array[Weather]): Weather = {
    if(weatherlist == null || weatherlist.length == 0) return null
    var low = 0
    var high = weatherlist.length - 1
    while(low <= high) {
      val mid = (low + high)/2
      val midVal = weatherlist(mid).timestamp
      if (midVal < ts) {
        low = mid + 1
      }
      else if (midVal > ts) {
        high = mid - 1
      }
      else {
        return weatherlist(mid)
      }
    }
    if(high == -1) return weatherlist(0)
    if(high == weatherlist.length - 1) return weatherlist.last

    if(ts-weatherlist(high).timestamp <= weatherlist(high+1).timestamp-ts) weatherlist(high) else weatherlist(high+1)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val paramsDf =
      hiveContext.sql(s"""
                    select
                    dynamic_interval
                    from
                    origindb.banma_cos__bm_feature_engineering_params
                    where
                    valid=1
        """)

    val di = (paramsDf.collect()(0)(0)).asInstanceOf[Long]
    val dynamic_interval = sc.broadcast(di)

    hiveContext.udf.register("date2datekey", date2datekey _)
    hiveContext.udf.register("timestamp", timestamp _)

    var dtClause: String = ""
    if(args.length == 1) {
      val dt = args(0)
      dtClause = s"and weather_date like '$dt%'"
    }
    else if(args.length == 2) {
      val dt1 = args(0)
      val dt2 = args(1)
      dtClause = s"and (weather_date between '$dt1 00:00' and '$dt2 23:59')"
    }

    val restrictClause = "and valid=1"
    val weatherDf =
      hiveContext.sql(s"""
                    select
                    city_id,
                    city_name,
                    weather_date,
                    current_weather,
                    timestamp(concat(weather_date,":00")) `ts`,
                    date2datekey(weather_date) `dt`
                    from
                    origindb.banma_cos__bm_dispatch_weather
                    where true $dtClause $restrictClause
        """.stripMargin)


    val weather_rdd = weatherDf.map(l => Weather(l.getLong(0), l.getString(1), l.getString(2), l.getString(3), l.getLong(4), l.getString(5)))
      .map(weather => ((weather.dt, weather.city_id, weather.city_name), weather))
      .groupByKey()
      .flatMap(
        l => {
          val k = l._1
          val v = l._2
          val weathers = v.toArray
          val weathers_sorted = weathers.sorted
          val n: Int = 24 * 60 / dynamic_interval.value.asInstanceOf[Int]
          val today_date = datekey2date(k._1)
          val begintimestamp = timestamp(s"$today_date 00:00:00")

          var ts_weather_ab = ArrayBuffer[(Long, Weather)]()
          for( i <- 0 until n) {
            val ts: Long = begintimestamp + i * dynamic_interval.value.asInstanceOf[Int] * 60
            val wea = what_weather(ts, weathers_sorted)
            ts_weather_ab += ((ts, wea))
          }
          val ts_weather_array = ts_weather_ab.toArray

          var ret = ArrayBuffer[(Long, String, String, String, Long)]()
          for( i <- ts_weather_array ) {
            // scheme: city_id city_name current_weather dt timestamp
            if(i._2!=null) ret += ((k._2, k._3, i._2.current_weather, k._1, i._1))
          }
          ret.iterator
        }
      )

    // 注册临时表
    weather_rdd.toDF("city_id", "city_name", "current_weather", "dt", "timestamp").registerTempTable("peisongpa_fact_weather_city_phase")

    // 写入天分区表
    hiveContext.sql("use mart_shangchao")
    hiveContext.sql("set hive.exec.dynamic.partition=true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nostrick")

    /*
    create table if not exists mart_shangchao.peisongpa_fact_weather_city_phase(city_id bigint comment '城市id', city_name string comment '城市名称', current_weather string comment '当前天气', timestamp bigint comment '时间戳') partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
     */
    hiveContext.sql("""insert overwrite table mart_shangchao.peisongpa_fact_weather_city_phase partition(dt) select city_id, city_name, current_weather, timestamp, dt from peisongpa_fact_weather_city_phase distribute by dt""")
  }
}
