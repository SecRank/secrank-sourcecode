package com.secrank.examples

import org.apache.spark.SparkContext.log
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{LongType, StructField}
import java.time.LocalDate
import org.apache.spark.sql.functions._
import java.net.URI
import scala.util.control.Breaks.{break, breakable}

object TopFQDNDailyRelease {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopFQDNDaily").setMaster("yarn")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("TopFQDNDaily").master("yarn").getOrCreate()
    import spark.implicits._

    // Input the date
    val gYear = args(0).toInt
    val gMon  = args(1).toInt
    val gDay  = args(2).toInt
    val gDate = "%04d-%02d-%02d".format(gYear, gMon, gDay)

    /* ========== Step 0. Preprocessing  (Please refer to Sec 6.1 in our paper) ========== */

    /* Please set the path to your own data path on HDFS (or the path on your machine).
     * The trends data path stores the daily PDNS traffic data table, and there are at least the following fields in the data frame:
     *     "fqdn": the domain name (the main key);
     *     "request_cnt": the number of total requests for the FQDN on one day;
     *     "client_cnt": the number of total IP addresses that requests the FQDN on one day. */
    val trends_path = "/trends/pdate=%04d%02d%02d".format(gYear, gMon, gDay)

    // Filter out those domains with #IPs < 15 and #Reqs < 15 for optimization.
    val filter_df = spark.read.parquet(trends_path)
      .filter(!$"fqdn".contains(".arpa")) // Filter out those rDNS domains.
      .filter($"rtype" === 1)   // Select those DNS records that are parsed successfully, i.e., rtype = 1 (can be neglected when applying this code),
      .filter($"request_cnt" >= 15 && $"client_cnt" >= 15)
      .withColumnRenamed("request_cnt", "req") // Rename
      .withColumnRenamed("client_cnt", "ip")   // Rename
      .withColumnRenamed("fqdn", "domain")     // Rename
      .select("domain", "req", "ip")

    // Save the data (rdd format) to the path /fqdn/filter/ for later use in Step 1.
    val filter_save_path = "/fqdn/filter/%s".format(gDate)
    val filter_rdd = filter_df.rdd.map(_.mkString("\t"))
    filter_rdd.saveAsTextFile(filter_save_path)

    /* =============================== End Step 0 =============================== */




    /* ========== Step 1. IP-Specific Domain Preferences Computing (Please refer to Sec 5.1 in our paper) ========== */

    // Input the rdd we just computed in Step 0 that has filtered out domains with #IPs < 15 and #Reqs < 15.
    val selected_domain = sc.textFile("/fqdn/filter/%s/".format(gDate)).map(_.split("\t"))
      .map(x => x(0)).toDF("selected_domain") // Rename for later table jointing.

    /* Input the access data that stores the daily access traffic table.
    *  Each row in the table represents a record that an IP address (client_ip) requested an FQDN (fqdn) in a day, and there are at least the following fields in the data frame:
    *  1) "fqdn": the domain name;
    *  2) "sld": the second-level domain of the fqdn;
    *  3) "client_ip": the IP address;
    *  4) "request_cnt": the number of requests that the IP address requests the fqdn on that day. */
    val access_df_pref = spark.read.parquet("/access/pdate=%s/".format("%04d%02d%02d".format(gYear, gMon, gDay)))
      .filter($"rtype" === 1)
      .select($"fqdn".alias("domain"), $"sld", $"client_ip".alias("ip"), $"request_cnt".alias("req"))

    // Select those domains with #IPs >= 15 and #Reqs >= 15 in the access data table.
    val access_filter_df = access_df_pref.join(selected_domain, selected_domain("selected_domain") === access_df_pref("domain"))

    // Compute the maximum value of Request Volume $\gamma$ for normalization.
    val max_cnt_df = access_filter_df.agg(max("req").alias("max_cnt"))
    max_cnt_df.write.parquet("/ip_pre/max_record/%s".format(gDate)) // Save intermediate results for data analysis.
    val max_cnt_df_read = spark.read.parquet("/ip_pre/max_record/%s".format(gDate))
    var max_cnt = max_cnt_df_read.rdd.map(x => x(0)).take(1)(0).asInstanceOf[Long] * 1.0
    max_cnt = math.log(max_cnt * 1.0 + 1.0) // Use the log1p function for data smoothing.

    /* Compute the Active Duration $\alpha$ (Please refer to Active Duration $\alpha$ in Sec 5.1 in our paper).
    *  Please first organize your access data into 10-minute intervals (Please refer to Sec 5.1 and Sec 6.1 in our paper).
    *
    *  Each row in one 10-minute interval (e.g., serial number 0 represents 00:00 - 00:10) in the data frame represents a record
    *  that an IP address (client_ip) has requested an FQDN (fqdn) during the specific 10-minute interval. */

    // Input the first 10-minute data first, and count the active duration.
    val interval_path = "/interval_access/pdate=%04d%02d%02d/pnum=%d".format(gYear, gMon, gDay, 0)
    var union_interval_df = spark.read.parquet(interval_path)
      .filter($"rtype" === 1)
      .withColumn("ad_count",lit(1.0)).select("fqdn", "client_ip","ad_count")

    // There are 144 (0 to 143) 10-minute interval in total in a day, we now count the 1 to 143 intervals.
    for (interval_num <- 1 to 143){
      val interval_path = "/interval_access/pdate=%04d%02d%02d/pnum=%d".format(gYear, gMon, gDay, interval_num)
      val temp_interval_df = spark.read.parquet(interval_path)
        .filter($"rtype" === 1)
        .withColumn("ad_count",lit(1.0))
        .select("fqdn", "client_ip", "ad_count")
      union_interval_df = union_interval_df.unionAll(temp_interval_df)
    }
    val preference_df = access_filter_df.join(union_interval_df, union_interval_df("fqdn") === access_filter_df("domain"))

    /* Compute the preference score (geometric mean of active duration and request volume) of each IP-domain pair,
    *  and save the data frame to the path /ip_pre/mean_value/. */
    preference_df.withColumn("nor_ad", $"ad"/144.0)        // Normalize Active Duration $\alpha$.
      .withColumn("nor_cnt", log($"req" * 1.0 + 1.0) / max_cnt) // Normalize Request Volume $\gamma$.
      .withColumn("mean", sqrt($"nor_ad" * $"nor_cnt"))         // Compute the Geometric Mean (Please refer to Combination in Sec 5.1).
      .select("domain", "ip", "req", "nor_ad", "nor_cnt", "mean")
      .write.parquet("/ip_pre/mean_value/%s/".format(gDate))

    /* =============================== End Step 1 =============================== */




    /* ========== Step 2. IP Address Weighting Computing (Please refer to Sec 5.2 in our paper) ========== */

    /* ========== Step 2.1 FQDN-related Statistics Computing (Please refer to IP Address Total Request Volume $\mu$ in Sec 5.2 in our paper ) ========== */
    /* The access_path stores daily access traffic data table.
    *  Each row in the table represents a record that an IP address (client_ip) requested an FQDN (fqdn) in a day, and there are at least the following fields in the data frame:
    *  "fqdn": the domain name;
    *  "sld": the second-level domain of the fqdn;
    *  "client_ip": the IP address;
    *  "request_cnt": the number of requests that the IP address requests the fqdn on that day. */
    val access_path = "/access/pdate=%s/".format("%04d%02d%02d".format(gYear,gMon,gDay))

    // Select necessary fields (i.e., fqdn and client_ip) for computing FQDN-related statistics.
    val access_df_fqdn = spark.read.parquet(access_path)
      .filter($"rtype" === 1)
      .select($"fqdn".alias("domain"), $"client_ip".alias("ip"), $"request_cnt".alias("req"))

    // Filter out those rDNS domains.
    val fqdn_filter_df = access_df_fqdn.filter(!$"domain".contains(".arpa"))

    /* For each IP address, we compute three statistics, and save the data frame to the path /ip_weight/fqdn_stat/ for the next step:
    *   1) the maximum number of requests the IP address requests for a specific FQDN (named as max_cnt);
    *   2) the number of requests to each FQDN the IP address makes (named as fqdn_cnt);
    *   3) the total number of requests (among all FQDNs) the IP makes (named as sum_cnt). */
    fqdn_filter_df.groupBy("ip")
      .agg(max("req").alias("max_cnt"), count("domain").alias("fqdn_cnt"), sum("req").alias("sum_cnt"))
      .write.parquet("/ip_weight/fqdn_stat/%s".format(gDate))

    /* ============================== End Step 2.1 ============================== */


    /* ========== Step 2.2 SLD-related Statistics Computing (Please refer to Domain Diversity $\delta$ in Sec 5.2 in our paper) ========== */

    // Again, we input the access data and select necessary field (i.e., sld and client_ip) for computing SLD-related statistics.
    val access_df_sld = spark.read.parquet(access_path)
      .filter($"rtype"===1)
      .select( $"sld", $"client_ip".alias("ip"))

    // Filter out those rDNS domains.
    val sld_filter_df = access_df_sld.filter(!$"sld".contains(".arpa"))

    /* For each IP address, we compute one statistic, and save the data frame to the path /ip_weight/sld_stat/ for the next step:
    *   1) the number of requests to each SLD the IP address makes (named as sld_cnt). */
    sld_filter_df.distinct() // Please remember to remove duplicate records as the original access data frame is based on FQDN.
      .groupBy("ip")
      .agg(count("sld").alias("sld_cnt"))
      .write.parquet("/ip_weight/sld_stat/%s".format(gDate))

    /* ============================== End Step 2.2 ============================== */


    /* ========== Step 2.3 Combination (Please refer to Combination in Sec 5.2 in our paper) ========== */

    // Input the statistics of each FQDN we just computed in Step 2.1.
    val fqdn_stat_path = "/ip_weight/fqdn_stat/%s".format(gDate)
    val fqdn_df = spark.read.parquet(fqdn_stat_path)

    // Input the statistics of each SLD we just computed in Step 2.2.
    val sld_stat_path = "/ip_weight/sld_stat/%s".format(gDate)
    val sld_df = spark.read.parquet(sld_stat_path)
      .withColumn("sld_ip", $"ip") // Rename for later table jointing.
      .select("sld_ip", "sld_cnt")

    // Normalize IP Address Total Request Volume $\mu$.
    var sum_max = fqdn_df.agg(max("sum_cnt")).rdd.map(x => x(0)).take(1)(0).asInstanceOf[Long] * 1.0
    sum_max = math.log(sum_max * 1.0 + 1.0) // Use log1p function for data smoothing.
    val nor_fqdn_df = fqdn_df.withColumn("nor_sum", log($"sum_cnt" * 1.0 + 1) / sum_max)  // Normalization.

    // Normalize Domain Diversity $\delta$.
    var sld_max = sld_df.agg(max("sld_cnt")).rdd.map(x => x(0)).take(1)(0).asInstanceOf[Long] * 1.0
    sld_max = math.log(sld_max * 1.0 + 1.0) // Use log1p function for data smoothing.
    val nor_sld_df = sld_df.withColumn("nor_sld", log($"sld_cnt" * 1.0 + 1) / sld_max)    // Normalization.

    // Compute the weight value of each IP address, and save the data frame to path /ip_weight/weight/
    val joint_df = nor_sld_df.join(nor_fqdn_df, nor_fqdn_df("ip") === nor_sld_df("sld_ip"), "inner")
    joint_df.withColumn("weight", sqrt($"nor_sld" * $"nor_sum"))
      .select("ip", "sld_cnt", "nor_sld", "sum_cnt", "nor_sum", "weight")
      .write.parquet("/ip_weight/weight/%s".format(gDate))

    /* ============================== End Step 2.3 ============================== */

    /* =============================== End Step 2 =============================== */




    /* ========== Step 3. Weighted Borda 1000 Voting (Please refer to Sec 5.3 and Sec 6.1.1 in our paper) ========== */

    // Input the geometric mean value (preference score) we computed in Step 1.
    val mean_df = spark.read.parquet("/ip_pre/mean_value/%s/".format(gDate))

    /* Rank the preference score.
     * If there is a tie, use the alphabetical order of domain names to break it. */
    val w = Window.partitionBy("ip").orderBy($"mean".desc, $"domain")
    val pre_df = mean_df.withColumn("index", row_number().over(w))

    // Input the IP weights we computed in Step 2.
    val weight_df = spark.read.parquet("/ip_weight/weight/%s".format(gDate))
      .withColumn("ip_w", $"ip").select("ip_w", "weight") // Rename for later table jointing.

    // Joint the two data frame to combine IP preferences and weights
    val join_df = pre_df.join(weight_df, weight_df("ip_w") === pre_df("ip"), "inner")
      .select("domain", "ip", "mean", "index", "weight")
    join_df.write.parquet("/joint_weight/%s".format(gDate))  // Save intermediate results for data analysis.

    // Input the joint data frame we just computed.
    val rank_df = spark.read.parquet("/joint_weight/%s/".format(gDate))

    // Rank FQDNs by the aggregated Borda 1000 values.
    val order_df = rank_df.withColumn("borda_1000", greatest(-$"index" + lit(1001.0), lit(0)) * $"weight")
                  .groupBy("domain")
                  .agg(sum("borda_1000").alias("sum_borda_1000"))
                  .orderBy($"sum_borda_1000".desc, $"domain") // If there is a tie, again use the alphabetical order of domain names to break it.

    // Save the full Ranking of Weighted Borda 1000 to the path /topdomain/fqdn/raw/all/
    order_df.rdd.zipWithIndex().map(tp => Row.merge(tp._1, Row(tp._2 + 1))).map(_.mkString("\t"))
                  .saveAsTextFile("/topdomain/fqdn/raw/all/%s/".format(gDate))

    // Get the top 1M of Weighted Borda 1000, and save the data frame to the path /topdomain/fqdn/raw/top1M/.
    val all_rank_path = "/topdomain/fqdn/raw/all/%s/".format(gDate) // Input the full ranking.
    val top_df = sc.textFile(all_rank_path)
                  .map(_.split("\t")).map(x => (x(0).toString, x(1).toString, x(2).toInt))
                  .toDF("domain", "score", "rank")
                  .filter($"rank" <= 1000000)
                  .repartition(1)
                  .sortWithinPartitions($"rank")
    top_df.rdd.map(_.mkString("\t"))
      .saveAsTextFile("/topdomain/fqdn/raw/top1M/%s/".format(gDate))

    /* ========================================== End Step 3 ========================================================= */


    /* Note: After the step 3, one can add any further filter processes for domains in the ranking,
     *      e.g., filter out CDN/API domains, to build custom rankings. */

  }
}