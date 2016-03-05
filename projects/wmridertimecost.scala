package com.meituan.shangchao

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by qianyuxiang on 16/1/19.
  */
object wmridertimecost {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    var dt = ""
    var point_dt = ""
    // 如果没有参数,重导所有历史
    if (args.length == 0) {
    } else if(args.length == 1) { // 跑某天数据 默认是$now.delta(1).datekey
      dt =  "and dt = '" + args(0) + "'"
    } else if(args.length == 2) { // 跑某些天数据 [begindatekey, enddatekey]
      dt = "and dt between '" + args(0) + "' and '" + args(1) +"'"
    }

    // 运单表
    val fact_waybill = hiveContext.sql(s"""
    select
    waybill_id,
    rider_id,
    waybill_rec_time,
    waybill_arrived_time,
    dt,
    platform_poi_id,
    platform_id

    from
    mart_peisongpa.fact_rider_waybill_track
    where is_prebook=0 $dt""".stripMargin)

    // 聚合所有的时间点
    val rdd_tmp1 = fact_waybill.map(l => {
      ((l.getString(4), l.getLong(1)), List(l.getLong(2), l.getLong(3)))
    }).reduceByKey((p1, p2) => {
      (p1:::p2).sorted.distinct
    })

    // 将时间点分段
    val rdd_tmp2 = rdd_tmp1.flatMap(l => {
      val key = l._1
      val value = l._2
      var res = List[((String, Long), (Long, Long))]()
      var itr = value.iterator
      var pre = itr.next
      while(itr.hasNext) {
        var cur = itr.next
        res .::= (key, (pre, cur))
        pre = cur
      }
      res
    })

    // p1时间段 是否在 p2时间段 内
    def isIn(p1 :Tuple2[Long, Long], p2 : Tuple2[Long, Long]) = {
      if(p1._1>=p2._1 && p1._2<=p2._2) true else false
    }

    // 跟原表连接
    val rdd_tmp3 = fact_waybill.map(l => {
      ((l.getString(4), l.getLong(1)), List(l.getLong(0), l.getLong(2), l.getLong(3)))
    }).join(rdd_tmp2)

    // 过滤掉分割出来的时间段不在运单取单时间和运单送达时间这个时间段内的部分
    val rdd_tmp4 = rdd_tmp3.filter(l => {
      val key = l._1
      val value1 = l._2._1
      val value2 = l._2._2
      isIn(value2, (value1(1), value1(2)))
    })

    // 聚合出各时间段所处的哪些运单
    val rdd_tmp5 =
      rdd_tmp4.map(l => {
        val dt = l._1._1
        val rider_id = l._1._2
        val phase_begin = l._2._2._1
        val phase_end = l._2._2._2

        val waybill_id = l._2._1(0)
        ((dt, rider_id, phase_begin, phase_end), List(waybill_id))
      }).reduceByKey(_:::_)

    // 转换出运单各时间段的时间成本
    val rdd_tmp6 =
      rdd_tmp5.map(l => {
        val dt = l._1._1
        val rider_id = l._1._2
        val begin = l._1._3
        val end = l._1._4
        val waybill_ids = l._2
        val time_cost = (end.toDouble - begin)/waybill_ids.length
        ((dt, rider_id, time_cost), waybill_ids)
      })

    // 根据运单聚合各时间段时间成本,得出总时间成本
    val rdd_tmp7 =
      rdd_tmp6.flatMap(l => {
        val dt = l._1._1
        val rider_id = l._1._2
        val time_cost = l._1._3
        val waybill_ids = l._2
        var res = List[((String, Long, Long), Double)]()
        for(waybill_id <- waybill_ids) {
          res .::= ((dt, rider_id, waybill_id), time_cost)
        }
        res
      }).reduceByKey(_+_).map(l => {
        (l._1._3, (l._1._2, l._2, l._1._1))
      }).distinct()

    // 补充运单所属POI
    val rdd_tmp8 =
      fact_waybill.map(l => {
        (l.getLong(0), (l.getLong(5), l.getLong(6)))
      }).distinct().join(rdd_tmp7).map(l => {
        (l._1, l._2._1._1, l._2._1._2, l._2._2._1, l._2._2._2, l._2._2._3)
      })

    // 注册临时表
    rdd_tmp8.toDF("waybill_id", "platform_poi_id", "platform_id", "rider_id", "timecost", "dt").registerTempTable("fact_waybill_timecost")

    // 写入天分区表
    hiveContext.sql("use mart_shangchao")
    hiveContext.sql("set hive.exec.dynamic.partition=true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nostrick")
    /*
    create table if not exists mart_peisongpa.fact_waybill_timecost(waybill_id bigint comment '运单ID', platform_id bigint comment '平台id', platform_poi_id bigint comment 'poi id', rider_id bigint comment '骑手ID', timecost double comment '时间成本') partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    create table if not exists mart_shangchao.fact_waybill_timecost(waybill_id bigint comment '运单ID', platform_id bigint comment '平台id', platform_poi_id bigint comment 'poi id', rider_id bigint comment '骑手ID', timecost double comment '时间成本') partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    create table if not exists test.fact_waybill_timecost(waybill_id bigint comment '运单ID', platform_poi_id bigint comment 'poi id', rider_id bigint comment '骑手ID', timecost double comment '时间成本') partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
     */
    hiveContext.sql("insert overwrite table mart_shangchao.fact_waybill_timecost partition(dt) select waybill_id, platform_id, platform_poi_id, rider_id, timecost, dt from fact_waybill_timecost distribute by dt")

  }
}
