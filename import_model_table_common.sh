#!/bin/sh
#################################################
###  功能:数据清洗
###  导入方式:通过hivesql查询导入model库
###  数据源:hive
###  结果:hive中model库
###  维护人:wdong
#################################################

#删除表在hive中的文件
function delete_table_file(){
    hadoop fs -rm -r ${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE} || :
    writeLog '=========delete_table_file==========='
}

#删除hive表
function delete_table(){
    hive -e "DROP TABLE IF EXISTS ${HIVE_DB}.${HIVE_TABLE};"
    writeLog "=========delete_table==========="
}

#创建hive表
function create_table(){
    hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS ${HIVE_DB}.${HIVE_TABLE} (
      $TABLE_COLUMNS
      )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    LOCATION '${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE}'" 
}


#创建hive表
function insert_table(){
    hive -e "
    INSERT OVERWRITE TABLE ${HIVE_DB}.${HIVE_TABLE}
    ${SELECT_SQL}" 
}

#增量导入hive表
function insert_table_update(){
    hive -e"
    INSERT INTO TABLE ${HIVE_DB}.${HIVE_TABLE}
    ${SELECT_SQL}
    "
}

# 清除原来的数据
function truncate_table(){
    # 清除原来的数据
     if [ "${IS_HBT}" = "hbt" ];
       then
        mysql -h${MYCAT_HOST} -u${MYCAT_USERNAME} -p${MYCAT_PASSWORD} -P${MYCAT_PORT} -e "USE ${MYCAT_HBT_DB};TRUNCATE TABLE ${MYCAT_TABLE};"
    else
        mysql -h${MYCAT_HOST} -u${MYCAT_USERNAME} -p${MYCAT_PASSWORD} -P${MYCAT_PORT} -e "USE ${MYCAT_DB};TRUNCATE TABLE ${MYCAT_TABLE};"
    fi
    #清除45上面的数据
    #mysql -h 192.168.90.45 -uroot  -pJXzg123.com -P3306 -e "USE diagnosisTeaching;TRUNCATE TABLE ${MYCAT_TABLE};"
}

# 按列导出高基表数据
function export_table_hbt(){
    IS_HBT="hbt"
    export_table
    IS_HBT=""
}

# 导出数据
function export_table(){
    if [ "${IS_HBT}" = "hbt" ];
           then
        sqoop export --connect ${MYCAT_HBT_URL} --username ${MYCAT_USERNAME} --password ${MYCAT_PASSWORD} \
                        --table ${MYCAT_TABLE} --export-dir ${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE} \
                        --input-fields-terminated-by '\001' --lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'
    else
        sqoop export --connect ${MYCAT_URL} --username ${MYCAT_USERNAME} --password ${MYCAT_PASSWORD} \
                        --table ${MYCAT_TABLE} --export-dir ${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE} \
                        --input-fields-terminated-by '\001' --lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'

    fi
}

# 按列导出高基表数据
function export_data_column_hbt(){
    IS_HBT="hbt"
    export_data_column
    IS_HBT=""
}

# 导出数据
function export_table_column(){

   if [ "${IS_HBT}" = "hbt" ];
       then
       sqoop export --connect ${MYCAT_HBT_URL} --username ${MYCAT_USERNAME} --password ${MYCAT_PASSWORD} \
        --table ${MYCAT_TABLE} --export-dir ${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE} \
        --input-fields-terminated-by '\001' --lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' \
        --columns ${HIVE_COLUMNS}
   else
    sqoop export --connect ${MYCAT_URL} --username ${MYCAT_USERNAME} --password ${MYCAT_PASSWORD} \
        --table ${MYCAT_TABLE} --export-dir ${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE} \
        --input-fields-terminated-by '\001' --lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' \
        --columns ${HIVE_COLUMNS}
   fi
}



#数据清洗
function data_cleaning(){
    if [ -n "${HIVE_TABLE}" ]; then
        writeLog "into data_cleaning"
        #删除表在hive中的文件
        delete_table_file
        writeLog "删除表在hive中的文件"
        #删除hive表
        delete_table
        writeLog "删除hive表"
        #创建hive表
        create_table
        writeLog "创建hive表"
        #导入数据到hive表
        insert_table
        writeLog "导入数据到hive表"
        if [ "${IS_DEBUG}" = "true" ] && [ -n "${DEBUG_DATA_PATH}" ] && [ ! -d "${DEBUG_DATA_PATH}" ];then
                hive -e "LOAD DATA LOCAL INPATH '${DEBUG_DATA_PATH}' INTO TABLE ${HIVE_DB}.${HIVE_TABLE};"
        fi
    fi
}

#创建db
function create_databases(){
    hive -e "create database IF NOT EXISTS ${HIVE_DB}"
}

function export_data(){
     writeLog "into export_data"
     writeLog "HIVE_DB:"${HIVE_DB}
     writeLog "HIVE_TABLE:"${HIVE_TABLE}
     writeLog "MYCAT_URL:"${MYCAT_URL}
     writeLog "MYCAT_USERNAME:"${MYCAT_USERNAME}
     writeLog "MYCAT_PASSWORD:"${MYCAT_PASSWORD}
     writeLog "MYCAT_TABLE:"${MYCAT_TABLE}
     writeLog "BASE_HIVE_DIR:"${BASE_HIVE_DIR}
     #创建日志路径
     log_start
     #数据清洗
     data_cleaning
     # 清除原来的数据
     truncate_table
     writeLog "清除原来的数据"
     # 导出数据
     export_table
     writeLog "导出数据"
     #写导入开始标志
     log_end
     #文件是否存在
     log_table
     #删除java文件
     finish
 }

 function export_data_column(){
     writeLog "into export_data"
     writeLog "HIVE_DB:"${HIVE_DB}
     writeLog "HIVE_TABLE:"${HIVE_TABLE}
     writeLog "MYCAT_URL:"${MYCAT_URL}
     writeLog "MYCAT_USERNAME:"${MYCAT_USERNAME}
     writeLog "MYCAT_PASSWORD:"${MYCAT_PASSWORD}
     writeLog "MYCAT_TABLE:"${MYCAT_TABLE}
     writeLog "BASE_HIVE_DIR:"${BASE_HIVE_DIR}
     #写导入开始标志
     log_start
     #数据清洗
     data_cleaning
     # 清除原来的数据
     truncate_table
     writeLog "清除原来的数据"
     # 导出数据
     export_table_column
     writeLog "导出数据"
     #写导入开始标志
     log_end
     #文件是否存在
     log_table
     #删除java文件
     finish
 }

