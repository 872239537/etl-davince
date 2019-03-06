#!/bin/sh
#################################################
###  功能:通用导入hive数据库
###  导入方式:全量导入
###  数据源:关系型数据库
###  结果:hive中raw库
###  运行条件:无,支持数据重跑
###  运行命令:sh import_table_common.sh [table]
###  维护人:wdong
#################################################

function import_db_to_hive(){
    if [ -z "$DRIVER" ]; then
        V_DRIVER=""
    else
        V_DRIVER="--driver ${DRIVER}"
    fi

    for ele in ${tables[*]}
    do
        table=${ele}
        MAP_COLUMN=${map["${table}"]}
        writeLog "${table}"
        writeLog "删除已存在数据"
        hadoop fs -rm -r ${BASE_HIVE_DIR}/${HIVE_DB}/${TABLE_PREFIX}${table} || :
        hadoop fs -rm -r /user/hdfs/${table} || :

        writeLog "删除已存在库表"
        hive -e "DROP TABLE IF EXISTS ${HIVE_DB}.${TABLE_PREFIX}${table}"

        writeLog "导入表结构"

        if [ "${SQLSERVER}" = "${DBTYPE}" ]; then
            if [ -z "$MAP_COLUMN" ]; then
             sqoop create-hive-table $V_DRIVER --connect "${SOURCE_JDBC_URL};database=${DATABASE};username=${SOURCE_USERNAME};password=${SOURCE_PASSWORD}" \
            --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
            --table ${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table}
            else
                sqoop create-hive-table $V_DRIVER --connect "${SOURCE_JDBC_URL};database=${DATABASE};username=${SOURCE_USERNAME};password=${SOURCE_PASSWORD}" \
                --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
                --table ${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table} \
                --map-column-hive $MAP_COLUMN
            fi
        else
            if [ -z "$MAP_COLUMN" ]; then
            writeLog "数据库链接：${SOURCE_JDBC_URL}"
             sqoop create-hive-table $V_DRIVER --connect ${SOURCE_JDBC_URL} --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
            --table ${TABLE_PRE}${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table}
            else
                writeLog "数据库链接：${SOURCE_JDBC_URL}"
                sqoop create-hive-table $V_DRIVER --connect ${SOURCE_JDBC_URL} --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
                --table ${TABLE_PRE}${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table} \
                --map-column-hive $MAP_COLUMN
            fi
        fi



        writeLog "将hive表设置为外部表"
        hive -e "ALTER TABLE ${HIVE_DB}.${TABLE_PREFIX}${table} SET TBLPROPERTIES ('EXTERNAL'='TRUE');"

        writeLog "修改hive表文件存储路径"

        hive -e "ALTER TABLE ${HIVE_DB}.${TABLE_PREFIX}${table} SET LOCATION '${BASE_HIVE_DIR}/${HIVE_DB}/${TABLE_PREFIX}${table}';"

        writeLog  "全量导入数据，列分隔符'\001'，null字符串'\\N'"
        if [ "${SQLSERVER}" = "${DBTYPE}" ]; then
            if [ -z "$MAP_COLUMN" ]; then
                sqoop import --hive-import $V_DRIVER --connect "${SOURCE_JDBC_URL};database=${DATABASE};username=${SOURCE_USERNAME};password=${SOURCE_PASSWORD}" \
                --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
                -m 1 --table ${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table} \
                --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --fields-terminated-by '\001'
            else
                sqoop import --hive-import $V_DRIVER --connect "${SOURCE_JDBC_URL};database=${DATABASE};username=${SOURCE_USERNAME};password=${SOURCE_PASSWORD}" \
                --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
                -m 1 --table ${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table} \
                --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --fields-terminated-by '\001' \
                --map-column-hive $MAP_COLUMN
            fi
        else
            if [ -z "$MAP_COLUMN" ]; then
                  sqoop import --hive-import $V_DRIVER --connect ${SOURCE_JDBC_URL} --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
                  -m 1 --table ${TABLE_PRE}${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table} \
                  --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --fields-terminated-by '\001'
            else
                sqoop import --hive-import $V_DRIVER --connect ${SOURCE_JDBC_URL} --username ${SOURCE_USERNAME} --password ${SOURCE_PASSWORD} \
                  -m 1 --table ${TABLE_PRE}${table} --hive-table ${HIVE_DB}.${TABLE_PREFIX}${table} \
                  --null-string '\\N' --null-non-string '\\N' --hive-drop-import-delims --fields-terminated-by '\001' \
                  --map-column-hive $MAP_COLUMN
            fi
        fi

        log_table ${table}
        finish
    done
}


function create_databases(){
    writeLog "创建db"
    hive -e "create database IF NOT EXISTS ${HIVE_DB}"
}
