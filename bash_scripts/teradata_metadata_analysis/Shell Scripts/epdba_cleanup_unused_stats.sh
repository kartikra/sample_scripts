#!/usr/bin/ksh

regionProfile="PROD_CO"


# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi
		
	
	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
		else
			. $HOMEDIR/region/PROD_"$region".profile
			rt_cd=$?
			if [ $rt_cd -ne 0 ]
			then
                echo "Profile file PROD_"$region".profile cannot be found, Exiting"
                exit 902
			fi
        fi
			


# STEP-2 Create Log File

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName
	
	
# STEP-3 Get List of STATS in Production Databases

	echo "SELECT TRIM(DatabaseName) || '|' || TRIM(TableName) || '|' ||  TRIM(StatsId) || '|' ||  TRIM(FieldIdList) || '|' ||  TRIM(ColumnName)
	from DBC.StatsV
	WHERE TRIM(DatabaseName) IN ('$prodMatReportDB','$prodReportDB','$prodKPBIReportDB')
	AND TRIM(ColumnName) IS NOT NULL;" > $TEMPDIR/"$regionProfile"_get_list_of_stats.sql


	rm -f $TEMPDIR/"$regionProfile"_get_list_of_stats.out
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$regionProfile"_get_list_of_stats.sql $TEMPDIR/"$regionProfile"_get_list_of_stats.out | tee -a  $logFileName
	
	rm -f $TEMPDIR/"$regionProfile"_get_list_of_stats_columns.out
	tail +2 $TEMPDIR/"$regionProfile"_get_list_of_stats.out | cut -f1,2,3,4,5 -d'|' | sort | uniq | while read -r line ; do
	
		dbName=`echo $line | cut -f1 -d '|'`
		tabName=`echo $line | cut -f2 -d '|'`
		statsId=`echo $line | cut -f3 -d '|'`
		fldList=`echo $line | cut -f4 -d '|'`
		columnList=`echo $line | cut -f5 -d '|'`

		occurence=`echo $columnList | sed 's/[^,]//g'  | awk '{ print length }'`
		totalFields=`expr $occurence + 1`
		
		if [ $occurence == "0" ]
		then
			echo "$dbName|$tabName|$columnList|$columnList|$statsId|$fldList|$totalFields" >> $TEMPDIR/"$regionProfile"_get_list_of_stats_columns.out
		else
			i=1		
			while [ $i -le $totalFields ]
			do
				columnName=`echo $columnList | cut -f$i -d,`
				echo "$dbName|$tabName|$columnName|$columnList|$statsId|$fldList|$totalFields" >> $TEMPDIR/"$regionProfile"_get_list_of_stats_columns.out
				i=`expr $i + 1`
			done
		fi
		
	done
	
	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o HCCLCO_USHARE.ANALYZE_DBCSTATS  -d $TEMPDIR/"$regionProfile"_get_list_of_stats_columns.out -l $logFileName 
	
	


