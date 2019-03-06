#!/bin/sh
#################################################
###  功能:一卡通系统原始数据导入
###  导入方式:全量导入
###  数据源:西南医科原始库
###  结果:hive中raw库
###  运行条件:无,支持数据重跑
###  运行命令:sh raw_xyb.sh [fun_name] 有fun_name参数,就执行fun_name方法
###  维护人:wdong
#################################################
source ../config_biz.sh
source config/config_xyb.sh
source ../logtools.sh
source ../import_raw_table_common.sh

#exec_dir

create_databases

function import_xyb_day(){
    tables=(${ROW_TABLES})
    import_db_to_hive
}


#处理历史数据
#
#function deal_echard_ttnsmflwind(){
##    import_ecard_ecexpressbackup
##    import_ecard_ecexpressquery
##    hive -e "CREATE TABLE IF NOT EXISTS raw.echard_deal_ttnsmflwind
##    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
##    LOCATION '${BASE_HIVE_DIR}/raw/echard_deal_ttnsmflwind'
##    as select * from raw.ecard_ecexpress_ttnsmflwing
##    "
##    hive -e "ALTER TABLE raw.echard_deal_ttnsmflwind SET TBLPROPERTIES ('EXTERNAL'='TRUE');"
##    TABLE_PREFIX="${TABLE_PREFIX_ECEXPRESSBACKUP}"
##    tables_temp=(${ROW_TABLES_ECEXPRESSBACKUP})
##    for ele in ${tables_temp[*]}
##    do
##        hive -e"
##        INSERT INTO TABLE raw.echard_deal_ttnsmflwind
##        select a.* from raw.ecard_ecexpressbackup_${ele} a
##        left join raw.echard_deal_ttnsmflwind b on a.flw_id=b.flw_id where b.flw_id is null
##        "
##    done
#
#    hive -e"
#        INSERT INTO TABLE raw.echard_deal_ttnsmflwind
#        select a.* from raw.ecard_ecexpressquery_ttnsmflwing a
#        left join raw.echard_deal_ttnsmflwind b on a.flw_id=b.flw_id where b.flw_id is null
#    "
#
#
#    hive -e"
#    INSERT INTO TABLE raw.echard_deal_ttnsmflwind
#    select a.* from raw.ecard_ecexpress_ttnsmflwing a
#    left join raw.echard_deal_ttnsmflwind b on a.flw_id=b.flw_id where b.flw_id is null
#    "
#}

#获取要处理的表
if [ $# == 1 ]; then
    $($1)
else
    #写导入开始标志
    log_start
    #处理表
     import_ecard_ECEXPRESS
    #写导入开始标志
    log_end
fi



