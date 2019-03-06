#!/bin/sh
#################################################
###  功能:校益宝数据库全局变量定义
###  维护人:wdong
#################################################
# 原始数据源信息
SOURCE_JDBC_URL=${SOURCE_XYB_JDBC_URL}
SOURCE_USERNAME=${SOURCE_XYB_USERNAME}
SOURCE_PASSWORD=${SOURCE_XYB_PASSWORD}

# 数据库
HIVE_DB=raw
TABLE_PRE=xyb.
#日志业务标识
RAW_DATABASE="xyb"
#raw中表前缀
TABLE_PREFIX="ods_${RAW_DATABASE}_"

ROW_TABLES="xyb_saas_merchant_order_detail
xyb_merchant
xyb_merchant_outer
xyb_saas_merchant_server
xyb_users
xyb_trial_class
bs_invite_code
xyb_district
xyb_pinke_activity
xyb_users_role
xyb_trial_class_student
xyb_merchant_order
xyb_merchant_pos
xyb_saas_server"