#!/bin/sh
#################################################
###  功能:log工具类
###  维护人:wdong
#################################################
function log_start(){
    writeLog "****************start******************"
    writeLog "import start ${RAW_DATABASE}"
}

function log_end(){
    writeLog "import end ${RAW_DATABASE}"
    writeLog "*****************end****************"
}

function log_table() {
    table="$(echo $1 | tr '[:upper:]' '[:lower:]')"
    hadoop fs -test -e ${BASE_HIVE_DIR}/${HIVE_DB}/${TABLE_PREFIX}${table}
    if [ $? -eq 0 ] ;then
        writeLog "excute import table result --> succ:${table}"
    else
        writeLog "excute import table result --> fail:${table}"
    fi
}

function writeLog(){
    if [  $? -eq 0  ]
    then
        fn_log_info "$@ sucessed."
    else
        fn_log_error "$@ failed."
    fi
}
function fn_log_info() {
    DATE_N=`date '+%Y年%m月%d日 %T'`
    USER_N=`whoami`
    echo -e "\033[32m[${DATE_N}] [${USER_N}] execute [$0] [INFO] $@  \033[0m" >> ${LOG_DIR}${RAW_DATABASE}/${RAW_DATABASE}_`date +%Y-%m-%d`.log 2>&1
}

function fn_log_error(){
    DATE_N=`date '+%Y年%m月%d日 %T'`
    USER_N=`whoami`
    echo -e "\033[41;37m[${DATE_N}] [${USER_N}] execute [$0] [ERROR] $@ \033[0m" >> ${LOG_DIR}${RAW_DATABASE}/${RAW_DATABASE}_`date +%Y-%m-%d`.log 2>&1
}

function exec_dir() {
    if [ ! -d "${LOG_DIR}${RAW_DATABASE}" ]; then
      mkdir -p ${LOG_DIR}${RAW_DATABASE}
    fi
    exec >> ${LOG_DIR}${RAW_DATABASE}/${RAW_DATABASE}_`date +%Y-%m-%d`.log 2>&1
}

function finish() {
    rm -rf *.java
    writeLog 'finish!'
}