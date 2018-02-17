#!/usr/bin/ksh

	# inputType = 1 or 2; (i)	1 - Script Files	2 - Validation Report
	# Refresh Table Flag  (t)   y - Alter or Create Tables
	# Refresh View Flag   (v)   y - Refresh Views
	
	# USAGE-1 Run Script File only for view refresh          	: ksh epdba_perform_witsChanges.sh -i 1 -v y
	# USAGE-2 Run Script File for both table and view refresh 	: ksh epdba_perform_witsChanges.sh -i 1 -t y -v y 
	# USAGE-3 Run Validation File 								: ksh epdba_perform_witsChanges.sh -i 2 -t y -v y 

# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi


# STEP-2 Create Log File

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName
	errorlogName="$scriptName"_errors-${dateforlog}.log

	witsChgList="nc__2014_feb11"
    regionProfile="VALID_NC"
	ticketNo="WO0000004268693"	
	parallelCount="10"
	customViewPurpose=" 2014"

# STEP-3 Get Input Parameters

	while getopts i:t:v:c:r:w: par
        do      case "$par" in
                i)      inFileType="$OPTARG";;
                t)      refTableInd="$OPTARG";;
                v)      refViewInd="$OPTARG";;
				c)      in_witsChgList="$OPTARG";;
                r)      in_regionProfile="$OPTARG";;
                w)      in_ticketNo="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh epdba_perform_witsChanges.sh -i <fileType> -t <refTableInd> -v <refViewInd> -c <wits change list> -r <region profile> -w <WO Number>"
                        exit 998;;
                esac
        done

	if [ -z "$inFileType" ]
	then
		echo "EXITING SCRIPT - File Type not provided "
		exit 901
	fi
	if [ -z "$refTableInd" ] && [ -z "$refViewInd" ]
	then
		echo "EXITING SCRIPT - Atleast 1 of the 2 indicators refTableInd or refViewInd must be specified as y"
		exit 902
	fi

	# Can also pass the parameters from the script	
	if [ ! -z "$in_witsChgList" ]
	then
		witsChgList=$in_witsChgList
	fi
	if [ ! -z "$in_regionProfile" ]
	then
		regionProfile=$in_regionProfile
	fi
	if [ ! -z "$in_ticketNo" ]
	then
		ticketNo=$in_ticketNo
	fi	

	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Preparing Scripts for WITS Refresh -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	
	
# STEP-4 Run the region profile

	if [ ! -f $HOMEDIR/region/"$regionProfile".profile ]
	then
		echo "EXITING Region Profile $HOMEDIR/region/$regionProfile.profile Not Found"
		exit 905
	fi

	
# STEP-5 Run Region Profile File
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

		
	echo "CALL CLARITY_DBA_MAINT.CLARITY_UPDATE_WITS_PROFILE ('$regionProfile','','$ticketNo','',ResultStr)" > $TEMPDIR/clarity_update_wits_profile.sql
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/clarity_update_wits_profile.sql $TEMPDIR/clarity_update_wits_profile.out | tee -a  $logFileName
	
	currBaselineRegion=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f2 -d'|'`
	ticketNo=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f3 -d'|'`
	currmanifestFile=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f4 -d'|'`

	if [ -z "$ticketNo" ]
	then
		echo "Workorder Not Found"
		exit 901
	fi

#--------------------------------------------------------------------------------------------------------------------------------------------------
			#-----   Functions  ---------




drop_table_if_exists () {

	rm -f $TEMPDIR/"$fileName"_drop_table_if_exists.*
	
	if [ -s $TEMPDIR/"$fileName"_tables_"$devStgDB".tmp ]
	then


		#echo " DELETE FROM CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST ALL;" > $TEMPDIR/"$fileName"_drop_table_if_exists.temp1 
		cat $TEMPDIR/"$fileName"_tables_"$devStgDB".tmp | cut -f2,4 -d'"' | sort | uniq | while read -r scriptLine ; 
		do
			witsDbName=`echo $scriptLine  | cut -f1 -d'"' | sed -e 's/\ //g'`
			witsTabName=`echo $scriptLine  | cut -f2 -d'" | sed -e 's/\ //g''`
			#echo " INSERT INTO CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST VALUES( '$witsDbName','$witsTabName' );" >> $TEMPDIR/"$fileName"_drop_table_if_exists.temp1 
			echo "$witsDbName|$witsTabName" >> $TEMPDIR/"$fileName"_drop_table_if_exists.temp1
		done
		
		$SCRIPTDIR/epdba_runFastLoad.sh -h $TDDEV -d $TEMPDIR/"$fileName"_drop_table_if_exists.temp1 -o CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST  -l $logFileName 


		echo "SELECT 'DROP TABLE ' || TRIM(DatabaseName) || '.\"' || TRIM(TableName) || '\";' FROM DBC.TablesV A " > $TEMPDIR/"$fileName"_drop_table_if_exists.temp1 
		echo " JOIN CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST B ON TRIM(A.DatabaseName)=B.ObjectDatabaseName AND TRIM(A.DatabaseName) IN ('$devStgDB','$devDeployStgDB1','$devDeployStgDB2','$devDeployStgDB3','$devDeployStgDB4','$devDeployStgDB5','$devDeployStgDB6','$devDeployStgDB7') "  >> $TEMPDIR/"$fileName"_drop_table_if_exists.temp1
		echo " AND TRIM(A.TableName)=B.ObjectTableName;" >> $TEMPDIR/"$fileName"_drop_table_if_exists.temp1 

		$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$fileName"_drop_table_if_exists.temp1 $TEMPDIR/"$fileName"_drop_table_if_exists.temp3 | tee -a  $logFileName


		if [ -f $TEMPDIR/"$fileName"_drop_table_if_exists.temp3 ]
		then
			sed '1,2d'  $TEMPDIR/"$fileName"_drop_table_if_exists.temp3 > $TEMPDIR/"$fileName"_drop_table_if_exists.sql

			if [ -f $TEMPDIR/"$fileName"_drop_table_if_exists.sql ]
			then
				#$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$fileName"_drop_table_if_exists.sql $TEMPDIR/"$fileName"_execute_drop_table_if_exists.out | tee -a  $logFileName
				mv $TEMPDIR/"$fileName"_drop_table_if_exists.sql $SQLDIR
				$SCRIPTDIR/epdba_runMultipleSQLFile.sh "$TDDEV" 15 $SQLDIR "$fileName"_drop_table_if_exists.sql $OUTDIR/drop_if_exists.out $logFileName	
				rm -f $SQLDIR/"$fileName"_drop_table_if_exists.sql 
			fi
		else
			echo "None of the Tables in $fileName exist. No Tables dropped" >> $logFileName
		fi


	fi

	rm -f $TEMPDIR/"$fileName"_drop_table_if_exists.*

}





backup_drop_table_if_exists () {

	rm -f $TEMPDIR/"$fileName"_backup_drop_table_if_exists.*
	rm -f $TEMPDIR/"$fileName"_restore_from_backup_table.sql
	rm -f $TEMPDIR/"$fileName"_backup_drop_backup_table.sql
	
	if [ -s $TEMPDIR/"$fileName"_tables_"$devReportDB".tmp ]
	then

		
		#echo " DELETE FROM CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST ALL;" > $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1 
		cat $TEMPDIR/"$fileName"_tables_"$devReportDB".tmp | cut -f2,4 -d'"' | sort | uniq | while read -r scriptLine ; 
		do
			witsDbName=`echo $scriptLine  | cut -f1 -d'"' | sed -e 's/\ //g'`
			witsTabName=`echo $scriptLine  | cut -f2 -d'" | sed -e 's/\ //g''`
			#echo " INSERT INTO CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST VALUES( '$witsDbName','$witsTabName' );" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1 
			echo "$witsDbName|$witsTabName" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1
		done
		
		$SCRIPTDIR/epdba_runFastLoad.sh -h $TDDEV -d $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1 -o CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST  -l $logFileName 
		
		

		echo "SELECT 'SELECT ''' || TRIM(DatabaseName) || '|' || TRIM(TableName)  || '|'', CAST(COUNT(*) AS BIGINT) FROM ' || TRIM(DatabaseName) || '.' || TRIM(TableName) || ';' FROM DBC.TablesV A " > $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1 
		echo " JOIN CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST B ON TRIM(A.DatabaseName)=B.ObjectDatabaseName AND TRIM(A.DatabaseName) IN ('$devReportDB','$devCalcReportDB1','$devCalcReportDB2','$devCalcReportDB3','$devCalcReportDB4','$devCalcReportDB5','$devCalcReportDB6','$devCalcReportDB7') "  >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1
		echo " AND (TRIM(A.TableName)= 'BKUP_' || B.ObjectTableName);" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1 

		echo "SELECT 'SELECT ''' || TRIM(DatabaseName) || '|' || TRIM(TableName)  || '|'', CAST(COUNT(*) AS BIGINT) FROM ' || TRIM(DatabaseName) || '.' || TRIM(TableName) || ';' FROM DBC.TablesV A " >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp11 
		echo " JOIN CLARITY_DBA_MAINT.CLARITY_REFRESH_OBJECT_LIST B ON TRIM(A.DatabaseName)=B.ObjectDatabaseName AND TRIM(A.DatabaseName) IN ('$devReportDB','$devCalcReportDB1','$devCalcReportDB2','$devCalcReportDB3','$devCalcReportDB4','$devCalcReportDB5','$devCalcReportDB6','$devCalcReportDB7') "  >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp11
		echo " AND (TRIM(A.TableName)=B.ObjectTableName);" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp11 		
		
		
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp1 $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp3 | tee -a  $logFileName
		sed '1,2d'  $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp3 > $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp4

		rm -f $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp3
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp11 $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp3 | tee -a  $logFileName
		sed '1,2d'  $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp3 >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp4
		

		if [ -f $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp4 ]
		then
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp4 $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp5  | tee -a  $logFileName

		
			if [ -f $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp5 ]
			then

				cat $TEMPDIR/"$fileName"_backup_drop_table_if_exists.temp5 | egrep -i "$devReportDB|$devCalcReportDB1|$devCalcReportDB2|$devCalcReportDB3|$devCalcReportDB4|$devCalcReportDB5|$devCalcReportDB6|$devCalcReportDB7" | grep -v "Count"  | while read -r scriptLine ;
				do
					witsDbName=`echo $scriptLine  | cut -f1 -d'|'  | sed -e 's/\ //g' | sed -e 's/\x27//g'`
					witsTabName=`echo $scriptLine  | cut -f2 -d'|' | sed -e 's/\ //g' | sed -e 's/\x27//g'`
					TabCount=`echo $scriptLine  | cut -f3 -d'|'  | sed -e 's/\ //g'`


					# Check if file is Count is non zero
					# Drop Backup Table , Create Backup Table
					if [ "$TabCount" != "0" ]
					then
						prefix=`echo $witsTabName | awk '{print substr($0,1,5)}'`
					
						if [ "$prefix" != "BKUP_" ]
						then
							echo "CREATE TABLE $witsDbName.\"BKUP_$witsTabName\" AS $witsDbName.\"$witsTabName\" WITH DATA AND STATS;" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.sql
							#echo "INSERT INTO $witsDbName.\"$witsTabName\" SELECT * FROM $witsDbName.\"BKUP_$witsTabName\";" >> $TEMPDIR/"$fileName"_restore_from_backup_table.sql
							echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_MIGSCRIPTS('$witsDbName','BKUP_$witsTabName','$witsDbName','$witsTabName',migSQL);" >> $TEMPDIR/"$fileName"_restore_from_backup_table.sql
							echo "DROP TABLE $witsDbName.\"$witsTabName\";" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.sql							
						else
							echo "DROP TABLE $witsDbName.\"$witsTabName\";" >> $TEMPDIR/"$fileName"_backup_drop_backup_table.sql
						fi
					else
						if [ ! -z "$witsDbName" ] && [ ! -z "$witsTabName" ] && [ ! -z "$TabCount" ] 
						then
							echo "DROP TABLE $witsDbName.\"$witsTabName\";" >> $TEMPDIR/"$fileName"_backup_drop_table_if_exists.sql
						fi
					fi

				done

			fi

			if [ -s $TEMPDIR/"$fileName"_backup_drop_backup_table.sql ]
			then
				$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$fileName"_backup_drop_backup_table.sql $TEMPDIR/"$fileName"_backup_drop_backup_table.out  | tee -a  $logFileName
			fi
			if [ -s $TEMPDIR/"$fileName"_backup_drop_table_if_exists.sql ]
			then
				$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$fileName"_backup_drop_table_if_exists.sql $TEMPDIR/"$fileName"_backup_drop_table_if_exists.out  | tee -a  $logFileName
			fi

		else
			echo "None of the Tables in $fileName exist. No Tables dropped" >> $logFileName
		fi

	fi

	rm -f $TEMPDIR/"$fileName"_backup_drop_table_if_exists.*

}




execute_script_file () {

	fileName=$2

	if [ -f "$1"/"$2" ]
	then

		rm -f $SQLDIR/$fileName
		
		# Remove Ctrl-M
		cat $1/$2 | tr -d \\r  > $SQLDIR/$fileName
		
		cd $SQLDIR

		# Replace Reporting DBName with  View DBNAME in all view statements
		viewCount=`cat $fileName | grep -i $devReportDB | grep -i -w "VIEW" | wc -l`
		if [ $viewCount -ne 0 ]
		then
			perl -pi -e 's/VIEW\ \"'$devReportDB'/VIEW\ \"'$devView'/g' $fileName
		fi
		

		# Comment out CALL Statements
		perl -pi -e 's/CALL HCCL/--CALL HCCL/g' $fileName
		perl -pi -e 's/CALL CLMS/--CALL CLMS/g' $fileName

		# Comment out EXIT Statements in SQL Files
		perl -pi -e 's/.if ERRORCODE/--.if ERRORCODE/g' $fileName

		
		# Check for CMPUPG_ prefix. If present skip the backup-drop process
		cmpugInd=`cat $fileName  | grep -i "CMPUPG" | wc -l` 

		if [ $cmpugInd -eq 0 ]
		then			
			# Comment out DROP Statements 
			perl -pi -e 's/DROP TABLE/--DROP TABLE/g' $fileName
		fi


		# Run the SQL File
		
		
		keywordCount=`cat $SQLDIR/$fileName  | grep -i "CREATE SET TABLE" | wc -l`
		
		if [ $parallelCount -gt 0 ] && [ $keywordCount -gt $parallelCount ]
		then
		
		
			$SCRIPTDIR/epdba_split_file.sh "CREATE SET TABLE" $SQLDIR/$fileName $parallelCount

			i="1"
			while [ $i -le $parallelCount ]
			do
				$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$fileName"_"$i"  "$TEMPDIR/"$fileName"_"$i".out" > "$logFileName"_"$i".log &
				sleep 2
				i=`expr $i + 1`
			done
			
			
			row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i "$fileName" | wc -l`
			while [ $row_cnt -gt 1 ]
			do
				sleep 10
				row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i "$fileName" | wc -l`
			done
			
			
			i="1"
			while [ $i -le $parallelCount ]
			do
				if [ -f ""$logFileName"_"$i".log" ]
				then
					cat ""$logFileName"_"$i".log" >> $logFileName
					rm -f ""$logFileName"_"$i".log"
				fi
				
				if [ -f "$TEMPDIR/"$fileName"_"$i".out" ]
				then
					cat "$TEMPDIR/"$fileName"_"$i".out" >> $TEMPDIR/"$fileName".out
					rm -f "$TEMPDIR/"$fileName"_"$i".out"
				fi
				
				if [ -f "$SQLDIR/"$fileName"_"$i"" ]
				then
					rm -f "$SQLDIR/"$fileName"_"$i""
				fi
				
				i=`expr $i + 1`
			done
		
		
		
		else
			$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/$fileName $TEMPDIR/"$fileName".out | tee -a  $logFileName
		fi
		
		rm -f $SQLDIR/$fileName
		rm -f $TEMPDIR/"$fileName".out
		
		
		# Restore from backup table if required
		if [ -s $TEMPDIR/"$fileName"_restore_from_backup_table.sql ]
		then			
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$fileName"_restore_from_backup_table.sql $TEMPDIR/"$fileName"_restore_from_backup_table_final.sql  | tee -a  $logFileName
			perl -pi -e 's/DataMigSQL/--DataMigSQL/g' $TEMPDIR/"$fileName"_restore_from_backup_table_final.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$fileName"_restore_from_backup_table_final.sql $TEMPDIR/"$fileName"_restore_from_backup_table.out  | tee -a  $logFileName
		fi

	else
		echo "Script File $2 not found in $1" >> $logFileName
	fi

}



alter_or_create_tables () {

	folderName=$1
	fileName=$2

	cd $folderName
	
	temp1=`echo $fileName | grep "Idx" | wc -l`		# Ignore Indexes
	temp2=`echo $fileName | grep -i "Procedure" | wc -l`	# Ignore Procedure


	if [ $temp1 -eq 0 ] && [ $temp2 -eq 0 ] 
	then
		echo "------------------------------------------------------------------"  >>  $logFileName
		echo "Starting Analysis for $fileName ... "  >>  $logFileName


		# Execute all CALL Statements having DROP_TABLE_IF_EXISTS

		cat $fileName | tr -d \\r |   grep -i "DROP_TABLE_IF_EXISTS" | cut -f2 -d'(' > $TEMPDIR/"$fileName"_drop_table_if_exists.tmp 
		perl -pi -e 's/\,/\./g' $TEMPDIR/"$fileName"_drop_table_if_exists.tmp
		perl -pi -e 's/\x27/\"/g' $TEMPDIR/"$fileName"_drop_table_if_exists.tmp

		cat $TEMPDIR/"$fileName"_drop_table_if_exists.tmp | egrep -i "$devStgDB|$devDeployStgDB1|$devDeployStgDB2|$devDeployStgDB3|$devDeployStgDB4|$devDeployStgDB5|$devDeployStgDB6|$devDeployStgDB7"  > $TEMPDIR/"$fileName"_tables_"$devStgDB".tmp
		cat $TEMPDIR/"$fileName"_drop_table_if_exists.tmp | egrep -i "$devReportDB|$devCalcReportDB1|$devCalcReportDB2|$devCalcReportDB3|$devCalcReportDB4|$devCalcReportDB5|$devCalcReportDB6|$devCalcReportDB7" > $TEMPDIR/"$fileName"_tables_"$devReportDB".tmp


		# STAGING TABLES being created or dropped

		# Identify List of Staging Tables being created
		cat $fileName | tr -d \\r  | egrep -i "$devStgDB|$devDeployStgDB1|$devDeployStgDB2|$devDeployStgDB3|$devDeployStgDB4|$devDeployStgDB5|$devDeployStgDB6|$devDeployStgDB7" \
		| grep -i -w "CREATE" | grep -i -w "TABLE" >> $TEMPDIR/"$fileName"_tables_"$devStgDB".tmp 
				
		
		# Identify List of Staging Tables being dropped
		cat $fileName | tr -d \\r  | egrep -i "$devStgDB|$devDeployStgDB1|$devDeployStgDB2|$devDeployStgDB3|$devDeployStgDB4|$devDeployStgDB5|$devDeployStgDB6|$devDeployStgDB7" \
		| grep -i -w "DROP" | grep -i -w "TABLE" >> $TEMPDIR/"$fileName"_tables_"$devStgDB".tmp 
		
		# Check for CMPUPG_ prefix. If present skip the backup-drop process
		cmpugInd=`cat $fileName  | grep -i "CMPUPG" | wc -l` 

		# Drop Staging Tables if they exist
		if [ $cmpugInd -eq 0 ]
		then
			drop_table_if_exists
		fi

		
		# REPORTING TABLES being created or dropped

		# Identify List of Reporting Tables being created
		cat $fileName | tr -d \\r | egrep -i "$devReportDB|$devCalcReportDB1|$devCalcReportDB2|$devCalcReportDB3|$devCalcReportDB4|$devCalcReportDB5|$devCalcReportDB6|$devCalcReportDB7" \
		| grep -i -w "CREATE" | grep -i -w "TABLE"  >> $TEMPDIR/"$fileName"_tables_"$devReportDB".tmp 

		# Identify List of Reporting Tables being dropped
		cat $fileName | tr -d \\r | egrep -i "$devReportDB|$devCalcReportDB1|$devCalcReportDB2|$devCalcReportDB3|$devCalcReportDB4|$devCalcReportDB5|$devCalcReportDB6|$devCalcReportDB7" \
		| grep -i -w "DROP" | grep -i -w "TABLE"  >> $TEMPDIR/"$fileName"_tables_"$devReportDB".tmp 
		
		# Backup and Drop Reporitng Tables if they exist
		if [ $cmpugInd -eq 0 ]
		then
			backup_drop_table_if_exists
		fi
		

		# Run the scripts in the file

		if [ -d $WITREFDIR/"$witsChgList" ]
		then
			execute_script_file "$WITREFDIR"/"$witsChgList" "$fileName" 
		else
			execute_script_file "$WITREFDIR" "$fileName"
		fi


	fi




}


create_view_analysis_file () {

	# All Reporting Tables 	- Refresh  View and User View

	cat $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat | egrep -i 'CREATE|ALTER' | cut -f2,3,4 -d'"' | sort | uniq > $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp
	cat $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat | grep -i 'RENAME' | cut -f6,7,8 -d'"' | sort | uniq >> $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp

	cat $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp | sed -e 's/\"//g' -e 's/\./\|/g' -e 's/'$'/\|TABLE/g' > $OUTDIR/"$witsChgList"_"$devReportDB"_WITS_UPDATE_VIEW_ANALYSIS.dat



	# All  Views 	- Refresh User Views
	# If not present create a view

	cat $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat | grep -i 'REPLACE' | cut -f2,3,4 -d'"' | sort | uniq > $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp
	cat $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp | sed -e 's/\"//g' -e 's/\./\|/g' -e 's/'$'/\|VIEW/g' >> $OUTDIR/"$witsChgList"_"$devReportDB"_WITS_UPDATE_VIEW_ANALYSIS.dat

	rm -f $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp


}



refresh_or_create_views () {


	
	# Load List of Tables in Analysis Table

	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDDEV -d $OUTDIR/"$witsChgList"_"$devReportDB"_WITS_UPDATE_VIEW_ANALYSIS.dat -o $ushareDB.WITS_UPDATE_VIEW_ANALYSIS  -l $logFileName 



	# Create all 6 output files by running view analysis bteq script

	rm -f $OUTDIR/"$devView"_new__views.dat
	rm -f $OUTDIR/"$devView"_refresh__views.sql
	rm -f $OUTDIR/"$devView"_existing_custom__views.dat
	rm -f $OUTDIR/"$devUserView"_new_user_views.dat
	rm -f $OUTDIR/"$devUserView"_refresh_user_views.sql
	rm -f $OUTDIR/"$devUserView"_existing_custom_user_views.dat
	rm -f $OUTDIR/"$devOtherDB1"_user_views.dat

	sed -e 's/'MY_VIEW_DB'/'$devView'/g' -e's/'MY_USHARE_DB'/'$ushareDB'/g'  -e 's/'MY_USERVIEW_DB'/'$devUserView'/g' \
	-e 's/'MY_REPORT_DB'/'$devReportDB'/g'  -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_OTHER_USERVIEW_DB'/'$devOtherDB1'/g'  $SQLDIR/accdba_wits_view_analysis.sql \
	> $SQLDIR/"$devUserView"_wits_view_analysis.sql

	echo "INSERT INTO $ushareDB.WITS_VIEW_ANALYSIS  SELECT * FROM $ushareDB.WITS_UPDATE_VIEW_ANALYSIS;" >> $SQLDIR/"$devUserView"_wits_view_analysis.sql
	$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devUserView"_wits_view_analysis.sql $OUTDIR/"$devUserView"_wits_view_analysis.out | tee -a  $logFileName

	rm -f $SQLDIR/"$devUserView"_wits_view_analysis.sql
	rm -f $OUTDIR/"$devUserView"_wits_view_analysis.out

	
	#---------------------------------------------------------------------------------------------------------------------------
	# Create New Structures for TPF Tables and Materialized View Tables
	
	rm -f $SQLDIR/tpfmatview_changes.sql
	rm -f $SQLDIR/tpfmatview_changes.out
	rm -f $OUTDIR/accdba_wits_tpfmatview_analysis.out
	
	$SCRIPTDIR/epdba_runSQLFile2.sh $TDDEV $SQLDIR/accdba_wits_tpfmatview_analysis.sql $OUTDIR/accdba_wits_tpfmatview_analysis.out | tee -a  $logFileName

	cat $OUTDIR/accdba_wits_tpfmatview_analysis.out | grep -v "QUERY_RESULT" | while  read -r line ; do
	
		inDB=`echo $line | cut -f1 -d'|'`
		inTable=`echo $line | cut -f2 -d'|'`
		outDB=`echo $line | cut -f3 -d'|'`
		outTable=`echo $line | cut -f4 -d'|'`
		
		if [ ! -z "$outTable" ]
		then
			rm -f $TEMPDIR/tpfmatview_analysis.out
			echo "SHOW TABLE $inDB.\"$inTable\";" > $TEMPDIR/tpfmatview_analysis.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDDEV $TEMPDIR/tpfmatview_analysis.sql $TEMPDIR/tpfmatview_analysis.out | tee -a  $logFileName
			if [ -s $TEMPDIR/tpfmatview_analysis.out ]
			then
				echo "DROP TABLE $outDB.\"$outTable\";" >> $SQLDIR/tpfmatview_changes.sql
				tail +1 $TEMPDIR/tpfmatview_analysis.out | sed -e 's/'$inDB'/'$outDB'/g' -e 's/'$inTable'/'$outTable'/g' >> $SQLDIR/tpfmatview_changes.sql
			fi
		fi
		
	done
	
	$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/tpfmatview_changes.sql $OUTDIR/tpfmatview_changes.out | tee -a  $logFileName

	
	#---------------------------------------------------------------------------------------------------------------------------
	# Refresh  Views
	
	if [ -f $OUTDIR/"$devView"_refresh__views.sql ] && [ -s $OUTDIR/"$devView"_refresh__views.sql ]
	then

		sed '1,2d' $OUTDIR/"$devView"_refresh__views.sql > $TEMPDIR/"$devView"_refresh__views.sql
		mv $TEMPDIR/"$devView"_refresh__views.sql $OUTDIR/"$devView"_refresh__views.sql

		$SCRIPTDIR/epdba_runSQLFile2.sh $TDDEV $OUTDIR/"$devView"_refresh__views.sql $TEMPDIR/"$devView"_refresh__views.sql | tee -a  $logFileName
		
		rm -f $SQLDIR/"$devView"_refresh__views.sql
		

		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$devView"_refresh__views.sql $SQLDIR/"$devView"_refresh__views.sql $ticketNo 

		$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devView"_refresh__views.sql $OUTDIR/"$devView"_refresh__views.out | tee -a  $logFileName

		rm -f $SQLDIR/"$devView"_refresh__views.sql
		rm -f $OUTDIR/"$devView"_refresh__views.out 

	fi




	# Create New  Views

	if [ -f $OUTDIR/"$devView"_new__views.dat ] && [ -s $OUTDIR/"$devView"_new__views.dat ]
	then
		sed '1,2d' $OUTDIR/"$devView"_new__views.dat > $TEMPDIR/"$devView"_new__views.dat
		mv $TEMPDIR/"$devView"_new__views.dat $OUTDIR/"$devView"_new__views.dat


		rm -f $SQLDIR/"$devView"_new__views.sql

		cat $OUTDIR/"$devView"_new__views.dat | while  read -r line ; do

			 dbName=`echo $line | cut -f1 -d'|'`
			tabName=`echo $line | cut -f2 -d'|'`


			sed -e 's/'MY_TGT_DB'/'$devView'/g'  -e 's/'MY_TGT_TAB'/'$tabName'/g' -e 's/'MY_SRC_TAB'/'$tabName'/g' \
			-e 's/'MY_SRC_DB'/'$devReportDB'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
			>> $SQLDIR/"$devView"_new__views.sql

		done

		$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devView"_new__views.sql $OUTDIR/"$devView"_new__views.out | tee -a  $logFileName

		rm -f $SQLDIR/"$devView"_new__views.sql
		rm -f $OUTDIR/"$devView"_new__views.out

	fi



	# Refresh  Views with Column Additions or Data Restrictions

	# $OUTDIR/"$devView"_existing_custom__views.dat



	#---------------------------------------------------------------------------------------------------------------------------


	# Refresh User Views
	
	if [ -f $OUTDIR/"$devUserView"_refresh_user_views.sql ] && [ -s $OUTDIR/"$devUserView"_refresh_user_views.sql ]
	then

		sed '1,2d' $OUTDIR/"$devUserView"_refresh_user_views.sql > $TEMPDIR/"$devUserView"_refresh_user_views.sql
		mv $TEMPDIR/"$devUserView"_refresh_user_views.sql $OUTDIR/"$devUserView"_refresh_user_views.sql

		$SCRIPTDIR/epdba_runSQLFile2.sh $TDDEV $OUTDIR/"$devUserView"_refresh_user_views.sql $TEMPDIR/"$devUserView"_refresh_user_views.sql | tee -a  $logFileName

		rm -f $SQLDIR/"$devUserView"_refresh_user_views.sql
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$devUserView"_refresh_user_views.sql $SQLDIR/"$devUserView"_refresh_user_views.sql $ticketNo 
		
		$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devUserView"_refresh_user_views.sql $OUTDIR/"$devUserView"_refresh_user_views.out | tee -a  $logFileName

		rm -f $SQLDIR/"$devUserView"_refresh_user_views.sql
		rm -f $OUTDIR/"$devUserView"_refresh_user_views.out 

	fi



	# Create New User Views

	if [ -f $OUTDIR/"$devUserView"_new_user_views.dat ] && [ -s $OUTDIR/"$devUserView"_new_user_views.dat ]
	then

		sed '1,2d' $OUTDIR/"$devUserView"_new_user_views.dat > $TEMPDIR/"$devUserView"_new_user_views.dat
		mv $TEMPDIR/"$devUserView"_new_user_views.dat $OUTDIR/"$devUserView"_new_user_views.dat


		rm -f $SQLDIR/"$devUserView"_new_user_views.sql

		cat $OUTDIR/"$devUserView"_new_user_views.dat | while  read -r line ; do

			 dbName=`echo $line | cut -f1 -d'|'`
			tabName=`echo $line | cut -f2 -d'|'`


			sed -e 's/'MY_TGT_DB'/'$devUserView'/g'  -e 's/'MY_TGT_TAB'/'$tabName'/g' -e 's/'MY_SRC_TAB'/'$tabName'/g' \
			-e 's/'MY_SRC_DB'/'$devView'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
			>> $SQLDIR/"$devUserView"_new_user_views.sql

		done

		$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devUserView"_new_user_views.sql $OUTDIR/"$devUserView"_new_user_views.out | tee -a  $logFileName

		rm -f $SQLDIR/"$devUserView"_new_user_views.sql
		rm -f $OUTDIR/"$devUserView"_new_user_views.out

	fi



	# Refresh User Views with Column Additions or Data Restrictions

	rm -f $SQLDIR/"$devUserView"_existing_custom_user_views.sql
	rm -f $SQLDIR/"$devUserView"_existing_custom_user_views_final.sql
	
	if [ -f $OUTDIR/"$devUserView"_existing_custom_user_views.dat ] && [ -s $OUTDIR/"$devUserView"_existing_custom_user_views.dat ]
	then
		sed '1,2d' $OUTDIR/"$devUserView"_existing_custom_user_views.dat > $TEMPDIR/"$devUserView"_existing_custom_user_views.dat
		mv $TEMPDIR/"$devUserView"_existing_custom_user_views.dat $OUTDIR/"$devUserView"_existing_custom_user_views.dat

		cat $OUTDIR/"$devUserView"_existing_custom_user_views.dat | cut -f1,2 -d'|' | sort | uniq | while  read -r line ; do


			userViewDB=`echo $line | cut -f1 -d'|'`
			table=`echo $line | cut -f2 -d'|'`
			

			# User View Exisits in WITS with Data Restrictions
			# Add the new columns to the view definition

			echo "SHOW VIEW $devUserView.\"$table\";" > $TEMPDIR/"$TDDEV"_userview.sql
			rm -f $TEMPDIR/"$TDDEV"_"$table"_userview.out
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$TDDEV"_userview.sql $TEMPDIR/"$TDDEV"_"$table"_userview.out | tee -a  $logFileName


			grep -w -n "FROM" $TEMPDIR/"$TDDEV"_"$table"_userview.out | grep -v '\-\-' | cut -f1 -d':' > $TEMPDIR/"$TDDEV"_"$table"_witsLine.out
			fileLength=`cat $TEMPDIR/"$TDDEV"_"$table"_userview.out | wc -l`

			head -1 $TEMPDIR/"$TDDEV"_"$table"_witsLine.out | while read -r line2 ; do

				headLine=`expr $line2 - 1`
				tailLine=`expr $fileLength - $line2 + 1`
				head -$headLine $TEMPDIR/"$TDDEV"_"$table"_userview.out > $TEMPDIR/"$TDDEV"_"$table"_userview.tmp

				echo "/* Addition of Columns in WITS */" >> $TEMPDIR/"$TDDEV"_"$table"_userview.tmp
				cat $OUTDIR/"$devUserView"_existing_custom_user_views.dat | grep -w -i $table | cut -f3 -d '|'  > $TEMPDIR/getcols.tmp

				cat $TEMPDIR/getcols.tmp | while read -r fieldName; do
					echo ",$fieldName"  >> $TEMPDIR/"$TDDEV"_"$table"_userview.tmp
				done

				tail -$tailLine $TEMPDIR/"$TDDEV"_"$table"_userview.out >> $TEMPDIR/"$TDDEV"_"$table"_userview.tmp
				mv $TEMPDIR/"$TDDEV"_"$table"_userview.tmp $TEMPDIR/"$TDDEV"_"$table"_userview.out

			done

			$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$TDDEV"_"$table"_userview.out $SQLDIR/"$devUserView"_existing_custom_user_views.sql $ticketNo 

		done
		
		rm -f $TEMPDIR/java_results.out
		cd $JAVADIR
		java -DoutputFile="$TEMPDIR/java_results.out" -DinputFile="$SQLDIR/"$devUserView"_existing_custom_user_views.sql" -DreasonText="$customViewPurpose" CustomViewFormatter
		mv $TEMPDIR/java_results.out $SQLDIR/"$devUserView"_existing_custom_user_views_final.sql

		# Running the custom View SQL
		# Run this file manually in case you see any issues - $SQLDIR/"$devUserView"_existing_custom_user_views_final.sql

		$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devUserView"_existing_custom_user_views_final.sql $OUTDIR/"$devUserView"_existing_custom_user_views_final.out | tee -a  $logFileName

	fi


	# Create Additional User Views

	if [ "$devrunName" == "CLMSNW1" ] || [ "$devrunName" == "CLMSNW2" ]
	then
		if [ -f $OUTDIR/"$devOtherDB1"_user_views.dat ] 
		then

			sed '1,2d' $OUTDIR/"$devOtherDB1"_user_views.dat > $TEMPDIR/"$devOtherDB1"_user_views.dat
			mv $TEMPDIR/"$devOtherDB1"_user_views.dat $OUTDIR/"$devOtherDB1"_user_views.dat


			rm -f $SQLDIR/"$devOtherDB1"_user_views.sql

			cat $OUTDIR/"$devOtherDB1"_user_views.dat | while  read -r line ; do

				 dbName=`echo $line | cut -f1 -d'|'`
				tabName=`echo $line | cut -f2 -d'|'`


				sed -e 's/'MY_TGT_DB'/'$devOtherDB1'/g'  -e 's/'MY_TGT_TAB'/'$tabName'/g' -e 's/'MY_SRC_TAB'/'$tabName'/g' \
				-e 's/'MY_SRC_DB'/'$devUserView'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
				>> $SQLDIR/"$devOtherDB1"_user_views.sql

			done

			$SCRIPTDIR/epdba_runSQLFile.sh $TDDEV $SQLDIR/"$devOtherDB1"_user_views.sql $OUTDIR/"$devOtherDB1"_user_views.out | tee -a  $logFileName

			rm -f $SQLDIR/"$devOtherDB1"_user_views.sql
			rm -f $OUTDIR/"$devOtherDB1"_user_views.out

		fi
	fi
	
}





#--------------------------------------------------------------------------------------------------------------------------------------------------
						#-----   Main Function  ---------
# STEP-5 Run the Main Function


	# SCRIPT File - Contains SQL Statements. Either a group of files with RUNME file or a single script file
	if [ $inFileType == "1" ]
	then		
	
		if [ -d $WITREFDIR/"$witsChgList" ]
		then
			echo "Found multiple files under $witsChgList - Locating RUNME.sql file ... "  >>  $logFileName

			rm -f $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat
			cd $WITREFDIR/"$witsChgList"

			if [ -f "RUNME.sql"  ]
			then

				rm -f $TEMPDIR/"$witsChgList"_"RUNME.sql"
						# tr -d \\r  removes Ctrl-M characters when moving from Windows to Unix
				cat "RUNME.sql" | grep -i ".run" | cut -f2 -d'=' | tr -d \\r  > $TEMPDIR/"$witsChgList"_"RUNME.sql"


				# Run the scripts for Table Changes
				cat $TEMPDIR/"$witsChgList"_"RUNME.sql" | while  read -r fileName ; do	
					cd $WITREFDIR/"$witsChgList"
					if [ -f "$fileName" ]
					then
					
						if [ "$refTableInd" == "y" ]
						then
							alter_or_create_tables $WITREFDIR/"$witsChgList" $fileName
						fi
						
						# Get List of CREATE TABLE, ALTER TABLE, RENAME and  VIEWS for analysis
						cd $WITREFDIR/"$witsChgList"
						cat $fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "CREATE" | grep -i -w "TABLE" >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat
						cat $fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "ALTER" | grep -i -w "TABLE"  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
						cat $fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "RENAME" | grep -i -w "TABLE"  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
						cat $fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "VIEW" | sed -e 's/'$devReportDB'/'$devView'/g'  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 

					fi

					rm -f $TEMPDIR/"$fileName"_*.tmp
				done

				# ADDITIONAL VIEW ANALYSIS
				if [ "$refViewInd" == "y" ]
				then
					create_view_analysis_file
					refresh_or_create_views
				fi

				rm -f $TEMPDIR/"$witsChgList"_"RUNME.sql"

			else
				echo "RUNME.sql Not Found !!"  >>  $logFileName
				exit 911
			fi

		else

			if [ -f $WITREFDIR/"$witsChgList" ]
			then
				echo "Found a Single file called $witsChgList "  >>  $logFileName

				# Run the scripts for Table Changes
				cd $WITREFDIR
				fileName="$witsChgList"
				if [ -f "$fileName" ]
				then
				
					if [ "$refTableInd" == "y" ]
					then
						alter_or_create_tables $WITREFDIR $fileName
					fi
					
					# Get List of CREATE TABLE, ALTER TABLE, RENAME and  VIEWS for analysis
					cat $WITREFDIR/$fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "CREATE" | grep -i -w "TABLE" >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat
					cat $WITREFDIR/$fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "ALTER" | grep -i -w "TABLE"  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
					cat $WITREFDIR/$fileName | tr -d \\r | grep -i $devReportDB | grep -i -w "RENAME" | grep -i -w "TABLE"  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
					cat $WITREFDIR/$fileName | tr -d \\r | egrep -i "$devReportDB|$devCalcReportDB1|$devCalcReportDB2|$devCalcReportDB3|$devCalcReportDB4|$devCalcReportDB5|$devCalcReportDB6|$devCalcReportDB7" | grep -i -w "VIEW" | sed -e 's/'$devReportDB'/'$devView'/g'  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
					cat $WITREFDIR/$fileName | tr -d \\r | egrep -i "$devDeployStgDB1|$devDeployStgDB2|$devDeployStgDB3|$devDeployStgDB4|$devDeployStgDB5|$devDeployStgDB6|$devDeployStgDB7" | grep -i -w "CREATE" | grep -i -w "TABLE" >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat
					cat $WITREFDIR/$fileName | tr -d \\r | egrep -i "$devDeployStgDB1|$devDeployStgDB2|$devDeployStgDB3|$devDeployStgDB4|$devDeployStgDB5|$devDeployStgDB6|$devDeployStgDB7" | grep -i -w "ALTER" | grep -i -w "TABLE"  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
					cat $WITREFDIR/$fileName | tr -d \\r | egrep -i "$devDeployStgDB1|$devDeployStgDB2|$devDeployStgDB3|$devDeployStgDB4|$devDeployStgDB5|$devDeployStgDB6|$devDeployStgDB7" | grep -i -w "RENAME" | grep -i -w "TABLE"  >> $TEMPDIR/"$witsChgList"_view_analysis_"$devReportDB".dat 
				fi

				# ADDITIONAL VIEW ANALYSIS
				if [ "$refViewInd" == "y" ]
				then
					create_view_analysis_file
					refresh_or_create_views
				fi
				
			else
					echo "Not Found any Script file or folder under $WITREFDIR called $witsChgList" 
					exit 911
			fi		
			
		fi
		
	fi

	# VALIDATION File - Generated by the DBAs to address action items from validation. 
	# File Contains 3 Fields - DatabaseName,Table Name, Action Item
	#   If Action Item = "Refresh View" then perform view analysis
	if [ $inFileType == "2" ]
	then	
	
		if [ -f $WITREFDIR/"$witsChgList" ]
		then

				echo "Found a Validation file called $witsChgList "  >>  $logFileName

				# Refresh Views Assigned in Validation
				cat $WITREFDIR/"$witsChgList" | grep -i "Refresh View" | cut -f2,3 -d'|'  > $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp
				cat $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp | sed -e 's/'$'/\|VIEW/g' > $OUTDIR/"$witsChgList"_"$devReportDB"_WITS_UPDATE_VIEW_ANALYSIS.dat
				rm -f $TEMPDIR/"$witsChgList"_final_view_analysis_"$devReportDB".tmp

				if [ "$refViewInd" == "y" ]
				then
					refresh_or_create_views
				fi
				
		else
			echo "Not Found any Validation file under $WITREFDIR called  $witsChgList" 
			exit 911
		fi

	fi

	rm -f $TEMPDIR/"$scriptName"_email_body.dat
	rm -f $TEMPDIR/"$scriptName"_email_attach.dat
	rm -f $LOGDIR/"$errorlogName".log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR $logName $LOGDIR/"$errorlogName".log

	
	
	
	emailSubjectLine="WITS REFRESH for file $witsChgList in $regionProfile Completed "
	if [ -s $LOGDIR/"$errorlogName".log ]
	then
		# Errors Found in Execution
		emailStatus="FAILURE"
		echo "$LOGDIR|"$errorlogName".log|"$scriptName"_errors_wits_refresh_run.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		echo "WITS REFRESH for file $witsChgList in $regionProfile completed with errors." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Scripts that failed during execution are attached to this email. Analyze them before proceeding to next step."  >>  $TEMPDIR/"$scriptName"_email_body.dat
	else
		
		emailStatus="SUCCESS"
		echo "WITS REFRESH for file $witsChgList in $regionProfile completed without any errors. Please proceed to next step" >> $TEMPDIR/"$scriptName"_email_body.dat
	fi

	
	if [  -s $SQLDIR/"$devUserView"_existing_custom_user_views_final.sql ]
	then
		cat $SQLDIR/"$devUserView"_existing_custom_user_views_final.sql >> $SQLDIR/"$devUserView"_existing_custom_user_views_combined.sql
		echo "$SQLDIR|"$devUserView"_existing_custom_user_views_final.sql|"$devUserView"_modified_custom_user_views.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		echo "Custom User View defintion has been changed to accomodate new fields in reporting database. Please see custom user views attached." >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	
	if [ $emailStatus != "FAILURE" ]
	then
		if [ -s "$TEMPDIR/"$scriptName"_email_attach.dat" ]
		then
			# Custom User View Needs to be attached
			$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
		else
			$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat"  -t "cd_bio_dba"
		fi
	else
		# FAILURE - Make sure only DBA group is notified
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
	fi






