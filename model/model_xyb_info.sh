#!/bin/sh
#################################################
###  功能:效益宝数据处理
###  数据源:raw
###  维护人:wdong
#################################################
source ../config.sh
source config/model_config_base.sh
source ../logtools.sh
source ../import_model_table_common.sh

exec_dir
create_databases




#效益宝  商户信息
function xyb_merchant(){
    HIVE_TABLE=${HIVE_TABLE_BASE_ACADEMY_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_ACADEMY_INFO}
    TABLE_COLUMNS="
       merchant_id string comment '商户ID',
       merchant_name string comment '商户名称',
       created_at string comment '商户开通时间',
       invite_code_edit string comment '维护码',
       channel_name string comment '维护人员',
       cityname string comment '城市名称'
    "
    SELECT_SQL="
		SELECT
          m.id as merchant_id ,
          m.merchant_name,
          FROM_UNIXTIME(m.created_at, 'yyyy-MM-dd') AS created_at,
          m.invite_code_edit,
          i.channel_name,tar.cityname

        FROM raw.ods_xyb_xyb_merchant m
        LEFT JOIN raw.ods_xyb_bs_invite_code i ON i.`code` = m.invite_code_edit
        LEFT JOIN raw.ods_date_tmp_test_phone tp ON m.phone = tp.test_phone
        LEFT JOIN raw.ods_xyb_xyb_district di1 ON i.city = di1.CODE
        LEFT JOIN raw.ods_date_tmp_xyb_target_manage tar ON di1.full_NAME = tar.cityname
        WHERE tp.test_phone IS NULL
            AND i.department = '交付与运营'
            AND m.invite_code < 100000
        GROUP BY
            m.id,m.merchant_name,
            FROM_UNIXTIME(m.created_at, 'yyyy-MM-dd'),
            m.invite_code_edit,
            i.channel_name,tar.cityname
    "
	data_cleaning
}


#效益宝  城市上月统计
function xyb_history_month_static_by_city(){
    HIVE_TABLE=${HIVE_TABLE_BASE_ACADEMY_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_ACADEMY_INFO}
    TABLE_COLUMNS="
       merchant_id string comment '商户ID',
       merchant_name string comment '商户名称',
       created_at string comment '商户开通时间',
       created_at string comment '维护码',
       created_at string comment '维护人员',
       created_at string comment '累计笔数',
       created_at string comment '累计GMV',
       created_at string comment '累计信用卡交易额占比',
       created_at string comment '本月笔数',
       created_at string comment '本月GMV',
       created_at string comment '上月笔数',
       created_at string comment '上月GMV',
       created_at string comment '本月信用卡分期GMV',
       created_at string comment '上月信用卡分期GMV',
       created_at string comment '本月花呗分期GMV',
       created_at string comment '上月花呗分期GMV',
    "
    SELECT_SQL="
		SELECT collegeno academy_no,
         collegename academy_name,
         NULL academy_type,
         NULL priority  from  raw.student_work_collegeinfo
		WHERE collegeno is NOT null
		ORDER BY  academy_no desc
    "
	HIVE_COLUMNS="academy_no,academy_name,academy_type,priority"
	export_data_column

}













#区域信息
function base_area_info(){
    HIVE_TABLE=${HIVE_TABLE_BASE_AREA_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_AREA_INFO}

    TABLE_COLUMNS="
       area_no string COMMENT '字典编号',
      area_name string COMMENT '字典名称',
      short_name string COMMENT '上级字典编号',
      priority string COMMENT '排序',
      parent_area_no string COMMENT '上级区域编号'
    "
    SELECT_SQL="
        SELECT
        '' as area_no,
        area_name as  area_name,
        '' as short_name,
        '' as priority,
        '' as parent_area_no
        from (
            SELECT
                case
                     when omitname='北京' then '北京市'
                     when omitname='天津' then '天津市'
                     when omitname='上海' then '上海市'
                     when omitname='重庆' then '重庆市'
                     else omitname
                 end  as area_name
            FROM raw.student_work_studentinfo
            WHERE omitname is NOT null
             AND omitname!='undefined'
        ) t
        group by area_name
    "
	HIVE_COLUMNS="area_no,area_name,short_name,priority,parent_area_no"
	export_data_column

}

#专业信息表
function base_major_info(){
    HIVE_TABLE=${HIVE_TABLE_BASE_MAJOR_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_MAJOR_INFO}

    TABLE_COLUMNS="
       academy_no string comment '学院系代码',
       major_no string comment '专业代码',
       major_name string comment '专业名称',
       priority string comment '排序'
    "
    SELECT_SQL="
		SELECT major.collegeno academy_no,
			         major.specialtyno major_no,
			         major.specialtyname major_name ,
			         NULL priority
			from raw.student_work_SpecialtyInfo major
			WHERE major.specialtyno is NOT null
    "
	HIVE_COLUMNS="academy_no,major_no,major_name,priority"
	export_data_column

}

#班级信息表  classinfo
function base_class_info(){
    HIVE_TABLE=${HIVE_TABLE_BASE_CLASS_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_CLASS_INFO}
    TABLE_COLUMNS="
       class_no string comment '班级代码',
       class_name string comment '班级名称',
       major_no string comment '专业编号',
       academy_no string comment '学院编号',
        grade string comment '年级'
    "
    SELECT_SQL="
			SELECT a.classno class_no,
			         a.classname class_name ,
			         a.specialtyno major_no,
			         b.collegeno academy_no,
			         a.specialtygrade grade
			FROM raw.student_work_classinfo a
			LEFT JOIN raw.student_work_SpecialtyInfo b
			    ON a.specialtyno=b.specialtyno 
    "
	HIVE_COLUMNS="class_no,class_name,major_no,academy_no,grade"
	export_data_column

}

#学院专业班级信息表  
function base_class_major_info(){
    HIVE_TABLE=${HIVE_TABLE_BASE_CLASS_MAJOR_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_CLASS_MAJOR_INFO}
    TABLE_COLUMNS="
       class_no string comment '班级代码',
       class_name string comment '班级名称',
       major_no string comment '专业编号',
       major_name string comment '专业名称',
       academy_no string comment '学院编号',
       academy_name string comment '学院名称',
        grade string comment '年级'
    "
    SELECT_SQL="
		SELECT a.classno class_no,
	         a.classname class_name ,
	         a.specialtyno major_no,
	         b.specialtyname major_name,
	         b.collegeno academy_no,
	         c.collegename academy_name,
	         a.specialtygrade grade
		FROM raw.student_work_classinfo a
		LEFT JOIN raw.student_work_SpecialtyInfo b
		    ON a.specialtyno=b.specialtyno
		LEFT JOIN raw.student_work_collegeinfo c
		    ON b.collegeno=c.collegeno
    "
	HIVE_COLUMNS="class_no,class_name,major_no,major_name,academy_no,academy_name,grade"
	export_data_column

}

#年级没信息表
function base_grade_info(){
    HIVE_TABLE=${HIVE_TABLE_BASE_GRADE_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_BASE_GRADE_INFO}
    TABLE_COLUMNS="
       grade string comment '学年'
    "
    SELECT_SQL="
          select  grade     
                from model.base_class_info group by grade
                ORDER BY grade
    "
	HIVE_COLUMNS="grade"
	export_data_column

}

#创建学期数据
function tmp_semester_info(){
    HIVE_DB=tmp
    HIVE_TABLE=tmp_semester_info

    delete_table_file
    delete_table

    hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS ${HIVE_DB}.${HIVE_TABLE}(
          semester_year STRING COMMENT '学年',
          semester STRING COMMENT '学期',
          begin_time TIMESTAMP COMMENT '开始时间',
          end_time TIMESTAMP COMMENT '结束时间',
          sort STRING COMMENT '排序'
      )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION '${BASE_HIVE_DIR}/${HIVE_DB}/${HIVE_TABLE}'"

    hive -e "LOAD DATA LOCAL INPATH '../file/semester_info.csv' INTO TABLE ${HIVE_DB}.${HIVE_TABLE};"

}

#导入学期数据
function base_semester_info(){
    tmp_semester_info
    HIVE_DB=model
    HIVE_TABLE=${HIVE_TABLE_SEMESTER_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_SEMESTER_INFO}

    TABLE_COLUMNS="semester_year STRING COMMENT '学年',
                 semester STRING COMMENT '学期',
                 begin_time TIMESTAMP COMMENT '开始时间',
                 end_time TIMESTAMP COMMENT '结束时间',
                 sort STRING COMMENT '排序',
                 create_time string comment '创建时间 格式 YYYYMMDDhhmmss'"
    SELECT_SQL="
        SELECT
        semester_year,semester,begin_time,end_time,
        row_number() over(order by begin_time desc ) sort,
        from_unixtime( unix_timestamp(),'yyyyMMddHHmmss' ) AS create_time
        FROM tmp.tmp_semester_info
        WHERE from_unixtime(unix_timestamp(),'yyyy-MM-dd') > begin_time
    "
    HIVE_COLUMNS="semester_year,semester,begin_time,end_time,priority"
    export_data_column
}

#高基表院系情况信息
function analysis_academy_info(){
    HIVE_TABLE=${HIVE_TABLE_ANALYSIS_ACADEMY_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_ANALYSIS_ACADEMY_INFO}

    year=`date -d "0 month ago" +%Y`
    month=`date -d "0 month ago" +%m`

    checkYear=$[year]
    startYear=$[year-1]
    if [ $month -gt 7 ]; then
        startYear=$[year]
        checkYear=$[year+1]
    fi
    if [ $# == 2 ] ; then
        checkYear=$1
        startYear=$2
    fi

    echo "checkYear--->$checkYear"
    echo "startYear--->$startYear"
    semester_year="${startYear}-${checkYear}"

    TABLE_COLUMNS="academy_no STRING COMMENT '学院编号',
                  academy_name STRING COMMENT '学院名称',
                  semester_year STRING COMMENT '学年',
                  priority STRING COMMENT '排序'"

     SELECT_SQL="SELECT t.collegeno AS academy_no,
                         t.collegename AS academy_name,
                         '${semester_year}' AS semester_year,
                         row_number() OVER(ORDER BY t.collegeno) AS priority
                FROM raw.student_work_collegeinfo t
                WHERE t.collegeno IS NOT NULL
                and t.collegeno != 68
                ORDER BY academy_no desc"

     HIVE_COLUMNS="academy_no,academy_name,semester_year,priority"

    IS_HBT="hbt"

    #数据清洗
    data_cleaning
    # 清除原来的数据
    mysql -h ${MYCAT_HOST} -u ${MYCAT_USERNAME} -p${MYCAT_PASSWORD} -P${MYCAT_PORT} -e "USE ${MYCAT_HBT_DB};delete  from ${MYCAT_TABLE} where semester_year='${semester_year}';"

    writeLog "清除原来的数据"
    # 导出数据
    export_table_column

    IS_HBT=""
}

#近五年的院系情况信息
function extend_analysis_academy_info(){

    year=`date -d "0 month ago" +%Y`
    month=`date -d "0 month ago" +%m`

    checkYear=$[year]
    startYear=$[year-1]
    if [ $month -gt 7 ]; then
        startYear=$[year]
        checkYear=$[year+1]
    fi

    analysis_academy_info "$[year-4]" "$[year-5]"
    analysis_academy_info "$[year-3]" "$[year-4]"
    analysis_academy_info "$[year-2]" "$[year-3]"
    analysis_academy_info "$[year-1]" "$[year-2]"
    analysis_academy_info "$[year]" "$[year-1]"
}

#专业情况
function analysis_major_info(){
    HIVE_TABLE=${HIVE_TABLE_ANALYSIS_MAJOR_INFO}
    MYCAT_TABLE=${MYCAT_TABLE_ANALYSIS_MAJOR_INFO}

    TABLE_COLUMNS="major_no STRING COMMENT '专业编号',
                  major_name STRING COMMENT '专业名称',
                  academy_no STRING COMMENT '学院名称',
                  academy_name STRING COMMENT '学院名称',
                  semester_year STRING COMMENT '学年',
                  priority STRING COMMENT '排序',
                  major_type STRING COMMENT '专业类型 本科专业，专科专业'"

    SELECT_SQL="SELECT major.specialtyno AS major_no,
                         major.specialtyname AS major_name,
                         academy.collegeno AS academy_no,
                         academy.collegename AS academy_name,
                         concat(major_grade.specialtygrade,'-',major_grade.specialtygrade+1) AS semester_year,
                         row_number() OVER(ORDER BY major.specialtyno) AS priority,
                        CASE major.spetype
                            WHEN 'B' THEN
                            '本科专业'
                            WHEN 'Z' THEN
                            '专科专业'
                            ELSE ' 其他专业 '
                        END AS major_type
                FROM raw.student_work_SpecialtyInfo major
                JOIN raw.student_work_collegeinfo academy
                    ON major.collegeno=academy.collegeno
                JOIN raw.student_work_spegradeinfo major_grade
                on major_grade.specialtyno=major.specialtyno
                WHERE major.specialtyno IS NOT NULL
                        AND academy.collegeno IS NOT NULL"
    HIVE_COLUMNS="major_no,major_name,academy_no,academy_name,semester_year,priority,major_type"
    export_data_column_hbt

}

create_databases
#获取要处理的表
if [ $# == 1 ]
    then
    $($1)
else
    #班级信息
    base_class_info
    #专业信息
    base_major_info
    #学院信息
    base_grade_info 
    #学院专业班级信息
    base_class_major_info
    #年级信息
    base_academy_info
    #学年学期数据
    base_semester_info
    #区域信息
    base_area_info



    #高基表近五年的院系情况信息
    analysis_academy_info
    extend_analysis_academy_info
    #专业情况
    analysis_major_info
fi