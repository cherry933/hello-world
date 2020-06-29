#!/bin/bash
. ~/build_where.sh
. ~/get_cinema_ip.sh
. ~/post_update.sh

instance_name=$1
company_code=$2
table_name=$3
no_download=$4
local_stage_folder=D:/HBMedia/VBS-Dev/SQL/Diagnostics/cine
dingxin_center_os_user=jiesuan
remote_stage_folder=/home/${dingxin_center_os_user}/stage
mysql_database=cine
etl_date_from='2020-06-01'
etl_date_to='2020-06-30'

ora_table_name=$table_name



if  test "$instance_name" = "test" 
then
	 echo "getting test"
apps_os_user=appltest
apps_db_password=appstest
host_name=hberptest.huayimedia.com


host_name_dingxin_center=10.108.0.1

elif  test "$instance_name" = "prod" 
then echo "getting prod"
apps_os_user=applprod
apps_db_password=qwer0987
host_name=hberpprod.huayimedia.com

host_name_dingxin_center=10.108.0.1
dingxin_db_user=cine_readonly
dingxin_db_password=dQxs6bEW
jspdb_os_user=jiesuan
jspdb_db_user=jiesuan
jspdb_host_name=10.80.3.196
jspdb_ora_stage_folder=/home/stage

elif  test "$instance_name" = "dev" 
then echo "getting dev"
apps_os_user=appldev
apps_db_password=appsdev
host_name=hberpdev.huayimedia.com
jspdb_os_user=jiesuan
jspdb_db_user=jiesuan
jspdb_host_name=10.80.3.190
jspdb_ora_stage_folder=/ASP_DB_TEST/stage

host_name_dingxin_center=10.108.0.1
dingxin_db_user=cine_readonly
dingxin_db_password=dQxs6bEW
settlement_db_host_name=127.0.0.1
settlement_db_user=root
settlement_db_password=abc12345
settlement_db_name=settlement

else
    echo "instance must either be dev, test or prod"
    exit 4
fi 


if [ -z "$company_code" ]; then
       echo "usage: cine.sh dev|test|prod 13601 [Table Name]"
exit
elif  test "$company_code" = "12801" 
then
mysql_database="center"
control_file_postfix=$company_code
else
control_file_postfix=""
fi
if [ -z "$table_name" ]; then
       echo "usage: cine.sh dev|test|prod 13601 [Table Name]"
exit
else
table_name=$(echo $table_name | tr '[:upper:]' '[:lower:]')
echo table is $table_name
fi
 

if   test  "$table_name" = "retail_categories_treepaths" 
then
ext_table_name="retail_categories_treepath_ext"
elif test "$table_name" = "cinema_quan_ticket_relation" 
then
ext_table_name="cinema_quan_ticket_relatio_ext"
elif test "$table_name" = "cinema_quan_barcode_order_segment_info" 
then
ext_table_name="cinema_quan_barcode_order__ext"
else
ext_table_name="${table_name}_ext"
fi
		

get_cinema_ip  

# echo company_name is $company_name
echo cinema number is $cinema_num
echo cinema ip is $cinema_ip

if [ -z "$cinema_ip" ]; then
       echo "unable to get server IP for $company_code $company_name"
exit
fi


build_where $mysql_database

if  ! test "$no_download" = "y" 
then
echo "step 2, dumping mysql from $cinema_ip"
ssh ${dingxin_center_os_user}@${host_name_dingxin_center} << ! 
/usr/local/cine/mysql/bin/mysqldump  -h  $cinema_ip -u${dingxin_db_user} -p${dingxin_db_password} --single-transaction $where_condition ${mysql_database} ${table_name}  | python ${python_processor} > ${remote_stage_folder}/${table_name}.tmp.txt
sed 's/"NULL"/""/g' ${remote_stage_folder}/${table_name}.tmp.txt >${remote_stage_folder}/${table_name}.txt


exit 0
!
else
echo "step 2, skip download"
fi
echo "step 3, sending DDL to JSP"
sftp -oBatchMode=no -b - ${jspdb_os_user}@${jspdb_host_name} << !
		lcd $local_stage_folder
    cd ${jspdb_ora_stage_folder}
    put $local_stage_folder/conversion/db/${table_name}${control_file_postfix}.sql
    put $local_stage_folder/conversion/db/${table_name}_ext${control_file_postfix}.sql
    bye
!




echo "step 4 importing to JSP"
build_where hbc${company_code} "and company_code='${company_code}'"

echo "ssh ${jspdb_os_user}@${jspdb_host_name}"


ssh ${jspdb_os_user}@${jspdb_host_name}  << !
sftp -oBatchMode=no -b - ${dingxin_center_os_user}@${host_name_dingxin_center} << endl
    lcd ${jspdb_ora_stage_folder}/data
    get ${remote_stage_folder}/${table_name}.txt
    bye
endl



if   test  "$table_name" = "cinema_quan_barcode_order_segment_info" 
then
ora_table_name="cinema_quan_barcode_order_segt"
ext_table_name="cinema_quan_barcode_order__ext"
else
ora_table_name=$table_name
fi



echo "INSERT into ${table_name} (SELECT '${company_code}','${cinema_num}', sysdate, x.* from ${ext_table_name} x);"
sqlplus hbc${company_code}/hbc${company_code} <<EOF
@${jspdb_ora_stage_folder}/${table_name}${control_file_postfix}.sql ${company_code} 
@${jspdb_ora_stage_folder}/${table_name}_ext${control_file_postfix}.sql ${company_code}
INSERT into ${table_name} (SELECT '${company_code}','${cinema_num}', sysdate, x.* from ${ext_table_name} x);
commit;
exit
EOF


if  test "$no_download" = "y" 
then
sqlplus hbc/hbc <<EOF
WHENEVER SQLERROR CONTINUE;
drop table ${table_name} purge;
exit
EOF
fi




echo "delete from ${table_name} where company_code = '${company_code}' ${where_clause};"
sqlplus hbc/hbc <<EOF
WHENEVER SQLERROR CONTINUE;
alter session set nls_date_format='yyyy-mm-dd hh24:mi:ss';
create table  ${table_name} as select * from hbc${company_code}.${table_name} where 1=2;
delete from ${table_name} where company_code = '${company_code}' ${where_clause};
INSERT into ${table_name} (SELECT  * from hbc${company_code}.${table_name} );
commit;

exit
EOF

exit 0
!
echo "do post update against table ${table_name}"
post_update