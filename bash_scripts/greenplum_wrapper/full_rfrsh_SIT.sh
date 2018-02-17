#!/usr/bin/ksh

run_stts=$1
bkptbl=$2
maintbl=$3
main_bkp=$bkptbl'1'
temp_bkptbl=`echo $bkptbl | cut -d '.' -f2`
temp_maintb1=`echo $maintbl |cut -d '.' -f2`
temp_main_bkp=`echo $main_bkp |cut -d '.' -f2`


#STEP-1 Run Profile File

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi

#STEP-2 Create Log File

shellName=`basename $0`
today=`date +"%Y_%m_%d_%H_%M_%S"`
logFile=`echo "$shellName-$today" | tr 'A-Z' 'a-z'`
logFileName=$LOGDIR/$logFile.log
touch $logFileName
rt_cd=$?
if [ $rt_cd -ne 0 ]
 then
   echo "ERROR - Unable to Create LogFile"
   exit 901
fi

chmod 775 $logFileName

#STEP-3 Create Backup Table SQL

if [ $run_stts == "S" ]
then
rm -f $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "DROP TABLE IF EXISTS $bkptbl ;" > $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "create table $bkptbl as (select * from $maintbl where 1=2);" >> $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "ALTER TABLE $bkptbl OWNER TO clm_rcvy_loader;" >> $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "GRANT ALL ON TABLE $bkptbl  TO clm_rcvy_loader;" >> $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "GRANT SELECT ON TABLE $bkptbl  TO clm_rcvy_read;" >> $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "GRANT SELECT ON TABLE $bkptbl  TO public;" >> $SQLDIR/full_rfrsh_bkp_create_SIT.sql
echo "GRANT ALL ON TABLE $bkptbl  TO clm_rcvy;" >> $SQLDIR/full_rfrsh_bkp_create_SIT.sql
chmod 775 $SQLDIR/full_rfrsh_bkp_create_SIT.sql
$SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f full_rfrsh_bkp_create_SIT.sql -l $logFileName
elif [ $run_stts == "E" ]
then
rm -f $SQLDIR/full_rfrsh_bkp_rename_SIT.sql
echo "ALTER TABLE $maintbl RENAME TO $temp_main_bkp;" > $SQLDIR/full_rfrsh_bkp_rename_SIT.sql
echo "ALTER TABLE $bkptbl RENAME TO $temp_maintb1;" >> $SQLDIR/full_rfrsh_bkp_rename_SIT.sql
echo "ALTER TABLE $main_bkp RENAME TO $temp_bkptbl;" >> $SQLDIR/full_rfrsh_bkp_rename_SIT.sql
chmod 775 $SQLDIR/full_rfrsh_bkp_rename_SIT.sql

$SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f full_rfrsh_bkp_rename_SIT.sql -l $logFileName
else
echo "Invalid run_stts value passed." >> $logFileName
fi

