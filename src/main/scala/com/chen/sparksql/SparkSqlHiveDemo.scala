package com.chen.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author bystander
 * @date 2020/8/31
 */
object SparkSqHiveDemo extends  App {

    private val spark: SparkSession = SparkSession
        .builder()
        .appName("spark sql hive demo")
        .master("local[4]")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nostrict")
        .getOrCreate()

    val sql = """
        select
            cast(y2.ymd_id as int) as ymd_id,
            y2.series_id,
            x3.date_week_id as ymw_id,
            x3.year_id,
            cast(cast(substr(cast(x4.date_day_id as string),1,4) as int)*100 + x4.week_of_year_number as int) as yw_id,
            x3.date_month_id as ym_id,
            cast(y2.exhibi_hall_flow as int) as exhibi_hall_flow,
            current_timestamp() as load_time
        from (
            select
            cast(t3.date_id as int) as ymd_id,
            -14000 as series_id,
            cast(t3.exhibi_hall_flow as int) as exhibi_hall_flow
            from (
                select t1.date_id,

                //TODO
                sum(case when t2.source_id_desc='展厅流量'
                    then  t1.new_opty_qty else t1.follow_up_opty_qty_site_first  end
                    ) +
                sum(case when t2.source_id_desc='展厅流量' then  t1.follow_up_opty_qty_site_first+t1.site_again_visit_qty
                    else t1.site_again_visit_qty  end
                    )
                     as exhibi_hall_flow

                from dh_mod.smart_tf_sale_analysis_chev t1
                --来源
                left join dh_mod.smart_tu_cust_business_rpt_source t2
                on
                    t1.cust_source_id = t2.source_id
                    and t1.cust_second_source_id = t2.second_source_id
                group by t1.date_id
            ) t3

            union all

            select
                date_id as ymd_id,
                -15000 as series_id,
                sum(showroom_qty) as exhibi_hall_flow
            from dh_mod.smart_tf_dealer_sale_overview
            where
                brand_ch_name ='别克'
                and date_id is not null
                and showroom_qty<>0
            group by date_id

            union all

            select
                t1.ymd_id,
                -46000 as series_id,
                cast(sum(t1.exhibi_hall_flow) as int) as exhibi_hall_flow
            from (
                select
                    cast(ymd_id as int) as ymd_id,
                    exhibi_hall_flow
                from dh_mod.mds_fdm_smart_brand_city_flow
                where brand_id = 46
            ) t1
            group by t1.ymd_id


            -- select
            -- 	cast(t1.ymd_id as int) as ymd_id,
            -- 	-46000 as series_id,
            -- 	cast(sum(exhibi_hall_flow) as int) as exhibi_hall_flow
            -- from dh_mod.mds_fdm_smart_brand_city_flow
            -- where
            -- 	brand_id = 46
            -- group by
            -- 	ymd_id
        ) y2
        left join dm_mod.tc_calender_smart x3 on y2.ymd_id=x3.date_day_id
        left join dm_mod.tc_calender_statics_smart x4 on y2.ymd_id = x4.date_day_id
        where
            y2.ymd_id <= (select max(update_time_id) from dm_mod.ts_update_data_time where data_type =6)
            and x3.date_day_id is not null
            and x4.date_day_id is not null;
        """

    private val df1: DataFrame = spark.sql(sql)
    df1.repartition(10)
        .write
        .mode("overwrite")
        .format("parquet")
        .insertInto("table_name")

}
