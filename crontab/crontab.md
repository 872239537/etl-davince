#2点到23点每隔5分钟执行一次
*/5 3-23 * * * cd /root/etl/raw &&  sh raw_xyb.sh increment_tables

10 0 * * * cd /root/etl/raw &&  sh raw_xyb_date_tmp.sh
