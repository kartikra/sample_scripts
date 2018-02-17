#!/usr/bin/ksh

#-----------------------------------------------------------------------   USAGE  ------------------------------------------------------------------------------------------
# Table  w/o Bkup : ksh epdba_copy_structure.sh -a tdp2.kp.org -b tdd1.kp.org -r SCAL_WITS -s HCCLPSC_TPF_T -t HCCLDSC8_TPF_T -w WO0000004010495 -o T -c 5       > log1.log & 
# Table with Bkup : ksh epdba_copy_structure.sh -a tdp2.kp.org -b tdd1.kp.org -r SCAL_WITS -s HCCLPSC_TPF_T -t HCCLDSC8_TPF_T -w WO0000004010495 -o T -c 5  -d Y > log1.log &
#  View  : ksh epdba_copy_structure.sh -a tdp2.kp.org -b tdd1.kp.org -r SCAL_WITS -s HCCLPSC -t HCCLDSC8 -w WO0000004010495 -o T -c 5 -m $HOME/dbmig/outfiles/setup_SCAL_WITS.list      > log1.log &
# User View  : ksh epdba_copy_structure.sh -a tdp2.kp.org -b tdd1.kp.org -r SCAL_WITS -s HCCLSC  -t HCCLSC   -w WO0000004010495 -o T -c 5 -m $HOME/dbmig/outfiles/setup_SCAL_WITS.list -u Y > log1.log &
# Procedure  : ksh epdba_copy_structure.sh -a tdp2.kp.org -b tdd1.kp.org -r SCAL_WITS -s HCCLPSCA_SP -t HCCLDSCA_SP -w WO0000004010495 -o P -c 5 -m $HOME/dbmig/outfiles/setup_SCAL_WITS.list > log2.log &
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#----------------------------------------------
#  exclusionValue (x)
#   1		Skip Source & Target DDL Export
#   2		Skip Target DDL Creation
#   5		Skip Script Execution
#   1+2=3	
#   1+5=6
#   2+5=7
#   1+2+5=8	Skip All 3. Need Only Validation
#-------------------------------------------------


# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi


# STEP-2 Read Input Parameters

 	while getopts a:b:r:s:t:w:o:c:m:d:u:x: par
        do      case "$par" in
				a) TDSRC="$OPTARG";;
				b) TDTGT="$OPTARG";;
				r) runName="$OPTARG";;
				s) srcDB="$OPTARG";;
				t) tgtDB="$OPTARG";;
				w) ticketNo="$OPTARG";;
				o) objType="$OPTARG";;
				c) parallelCount="$OPTARG";;
				m) migProfile="$OPTARG";;
				d) backupFlag="$OPTARG";;
				u) userViewFlag="$OPTARG";;
				x) exclusionValue="$OPTARG";;
                [?])    echo "Correct Usage -->  ksh epdba_copy_structure.sh -a <TDSRC> -b <TDTGT> -r <EnvName> -s <sourceDB> -t <targetDB> -w <ticketNo> -o <objType T or V> -c <parallelCount> -m <migProfile File> -u <userView Flag>"
                        exit 998;;
                esac
        done
		

	if [ -z "$exclusionValue" ]
	then
		exclusionValue="0"
	fi
	if [ -z "$backupFlag" ]
	then
		backupFlag="N" 
	fi
	
	
# STEP-3 Create the LogFile

	logName=""$runName"_copy_"$srcDB"_to_"$tgtDB".log"
	logFileName=$LOGDIR/archive/$logName

	rm -f $logFileName


# STEP-4 Load List of Tables to be Migrated
	
	rm -f $TEMPDIR/"$tgtDB"_get_migration_list_results.out
	echo "SELECT  '$runName|' || TRIM(DatabaseName) || '|' || TRIM(TableName) FROM DBC.TablesV WHERE TableName IS NOT NULL AND TableKind IN ('T', 'V') "  > $TEMPDIR/"$tgtDB"_get_migration_list.sql
	echo " AND TRIM(tablename) NOT LIKE ALL ('3/_%','BF%','%_Error1','%_Error2','TokenX_%') ESCAPE '/' "  >> $TEMPDIR/"$tgtDB"_get_migration_list.sql
	echo " AND TRIM(DatabaseName) = '$srcDB' " >> $TEMPDIR/"$tgtDB"_get_migration_list.sql		
	
	$SCRIPTDIR/epdba_runSQLFile2.sh $TDSRC $TEMPDIR/"$tgtDB"_get_migration_list.sql	$TEMPDIR/"$tgtDB"_get_migration_list_results.out > $logFileName
	sed '1d' $TEMPDIR/"$tgtDB"_get_migration_list_results.out > $TEMPDIR/"$tgtDB"_get_migration_list_results.tmp
	mv $TEMPDIR/"$tgtDB"_get_migration_list_results.tmp  $TEMPDIR/"$tgtDB"_get_migration_list_results.out 
	
	
	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -d $TEMPDIR/"$tgtDB"_get_migration_list_results.out -o CLARITY_DBA_MAINT.MIGRATION_LIST_LOAD  | tee -a $logFileName
	
	echo "DELETE FROM CLARITY_DBA_MAINT.MIGRATION_LIST WHERE ENV='$runName' AND DBNAME='$srcDB';" > $TEMPDIR/"$tgtDB"_load_migration_list.sql
	echo "INSERT INTO CLARITY_DBA_MAINT.MIGRATION_LIST SELECT ENV,DBNAME,TABNAME FROM CLARITY_DBA_MAINT.MIGRATION_LIST_LOAD WHERE ENV='$runName' AND DBNAME='$srcDB';" >> $TEMPDIR/"$tgtDB"_load_migration_list.sql
	
	$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $TEMPDIR/"$tgtDB"_load_migration_list.sql $TEMPDIR/"$tgtDB"_load_migration_list.out | tee -a  $logFileName

	
	if [ "$exclusionValue" != "1" ] && [ "$exclusionValue" != "3" ] && [ "$exclusionValue" != "6" ] && [ "$exclusionValue" != "8" ]
	then
	
	# STEP-5 Backup the Target Defintions 
		
		$SCRIPTDIR/epdba_export_ddl.sh -h $TDTGT -d $tgtDB -o ""$runName"_backup_"$tgtDB".sql"  >> $logFileName 
		

	# STEP-6 Export the Source Defintion
		
		$SCRIPTDIR/epdba_export_ddl.sh -h $TDSRC -d $srcDB -o ""$runName"_export_"$srcDB".sql"  >> $logFileName

	fi
	
	
	# STEP-7 Create the Target Defintion
	
	if [ "$exclusionValue" != "2" ] && [ "$exclusionValue" != "3" ] && [ "$exclusionValue" != "7" ] && [ "$exclusionValue" != "8" ]
	then
	
		# View Creation Script
		if [ "$objType" == "V" ]
		then
			# Create the Target Defintions by replacing all the database names in source definition based on migration profile (Views)
			rm -f $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql
			
			if [  "$userViewFlag" == "Y" ]
			then
				$SCRIPTDIR/epdba_create_target_view.sh $DDLBACKUPDIR/"$runName"_export_"$srcDB".sql $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql $ticketNo "$migProfile" 2
			else
				$SCRIPTDIR/epdba_create_target_view.sh $DDLBACKUPDIR/"$runName"_export_"$srcDB".sql $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql $ticketNo "$migProfile" 
			fi
			
			# Cleanup only those views that are present in target but not in source
			echo "SELECT 'DROP VIEW ' || '$tgtDB' || '."' || TableName || '";' FROM
			( 
			   SELECT TRIM(TableName) AS TableName
			   FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$tgtDB') AND TableKind='V'
			   MINUS
			   SELECT TRIM(TABNAME)
			   FROM CLARITY_DBA_MAINT.MIGRATION_LIST WHERE TRIM(DBNAME)IN ('$srcDB') AND ENV='$runName'
			)TEMP;" > $TEMPDIR/get_migration_view_list.sql
			
			rm -f $TEMPDIR/get_migration_view_list_results.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/get_migration_view_list.sql  $TEMPDIR/get_migration_view_list_results.sql >> $logFileName
			if [ -f $TEMPDIR/get_migration_view_list_results.sql ]
			then
				sed '1d' $TEMPDIR/get_migration_view_list_results.sql > $SQLDIR/"$runName"_cleanup_"$tgtDB".sql
			fi
			if [ -f $SQLDIR/"$runName"_cleanup_"$tgtDB".sql ]
			then
				$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDTGT 18 $SQLDIR "$runName"_cleanup_"$tgtDB".sql  $TEMPDIR/"$dbName"_list_db_maintenace.out $logFileName 	
			fi
			
			rm -f $SQLDIR/"$runName"_cleanup_"$tgtDB".sql
		fi
		
		
		# Procedure Creation Script
		if [ "$objType" == "P" ]
		then
			# Create the Target Defintions by replacing all the database names in source definition based on migration profile (Views)
			rm -f $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql
			#$SCRIPTDIR/epdba_create_target_view.sh $DDLBACKUPDIR/"$runName"_export_"$srcDB".sql $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql $ticketNo "$migProfile"
			
			cp $DDLBACKUPDIR/"$runName"_export_"$srcDB".sql $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql
			perl -pi -e 's/'CREATE\ *PROCEDURE'/'REPLACE\ PROCEDURE'/gi'  $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql 
			cat $migProfile | while read -r chgList ; do
				srcObject=`echo $chgList | cut -f1 -d '|'`
				tgtObject=`echo $chgList | cut -f2 -d '|'`
				objType=`echo $chgList | cut -f3 -d '|'`
				if [ "$objType" == "T" ]
				then
					perl -pi -e 's/'$srcObject'/'$tgtObject'/gi'  $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql  
					perl -pi -e 's/'$tgtObject'\.\ /'$tgtObject'\./gi'  $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql 
				fi
			done
		
			# Cleanup only those views that are present in target but not in source
			echo "SELECT 'DROP PROCEDURE ' || '$tgtDB' || '.\"' || TableName || '\";' FROM
			( 
			   SELECT TRIM(TableName) AS TableName
			   FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$tgtDB') AND TableKind='P'
				MINUS
			   SELECT TRIM(TABNAME)
			   FROM CLARITY_DBA_MAINT.MIGRATION_LIST WHERE TRIM(DBNAME)IN ('$srcDB') AND ENV='$runName'
			)TEMP;" > $TEMPDIR/get_migration_procedure_list.sql
			
			
			rm -f $TEMPDIR/get_migration_procedure_list_results.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/get_migration_procedure_list.sql  $TEMPDIR/get_migration_procedure_list_results.sql >> $logFileName
			if [ -f $TEMPDIR/get_migration_procedure_list_results.sql ]
			then
				sed '1d' $TEMPDIR/get_migration_procedure_list_results.sql > $SQLDIR/"$runName"_cleanup_"$tgtDB".sql
			fi
			if [ -f $SQLDIR/"$runName"_cleanup_"$tgtDB".sql ]
			then
				$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDTGT 18 $SQLDIR "$runName"_cleanup_"$tgtDB".sql  $TEMPDIR/"$dbName"_list_db_maintenace.out $logFileName 	
			fi
			rm -f $SQLDIR/"$runName"_cleanup_"$tgtDB".sql

			
			# Generate Complie Statement for all Procedures
			echo "SELECT 'ALTER PROCEDURE ' || '$tgtDB' || '.\"' || TableName || '\" COMPILE;' FROM
			( 
			   SELECT TRIM(TABNAME) AS TableName
			   FROM CLARITY_DBA_MAINT.MIGRATION_LIST WHERE TRIM(DBNAME)IN ('$srcDB') AND ENV='$runName'
			)TEMP;" > $TEMPDIR/get_migration_alter_procedure_list.sql
			rm -f $TEMPDIR/get_migration_alter_procedure_list_results.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/get_migration_alter_procedure_list.sql  $TEMPDIR/get_migration_alter_procedure_list_results.sql >> $logFileName
			if [ -f $TEMPDIR/get_migration_alter_procedure_list_results.sql ]
			then
				sed '1d' $TEMPDIR/get_migration_alter_procedure_list_results.sql > $SQLDIR/"$runName"_alter_procedure_"$tgtDB".sql
			fi
			
		fi
		
		
		# Table Creation Script
		if [ "$objType" == "T" ]
		then

			# Replace the databasename for Create Table
			sed -e 's/'$srcDB'/'$tgtDB'/g' $DDLBACKUPDIR/"$runName"_export_"$srcDB".sql  > $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql 

			
			if [ "$backupFlag" == "Y" ]
			then
			
				echo "SELECT  'SELECT ''' ||  TRIM(DatabaseName) || '|' || TRIM(TableName) || '|' ||  ''' || TRIM(COUNT(*)) AS RESULT_LINE FROM ' || 
				 TRIM(DatabaseName) || '.' || TRIM(TableName) || ';'
				FROM DBC.TablesV
				WHERE TRIM(DatabaseName) IN ('$tgtDB');" > $TEMPDIR/"$tgtDB"_get_table_count.sql

				rm -f $TEMPDIR/"$tgtDB"_get_table_count.out
				$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/"$tgtDB"_get_table_count.sql  $TEMPDIR/"$tgtDB"_get_table_count.out  >> $logFileName
				
				if [ -s $TEMPDIR/"$tgtDB"_get_table_count.out ]
				then
					sed '1d' $TEMPDIR/"$tgtDB"_get_table_count.out > $TEMPDIR/"$tgtDB"_get_final_table_count.sql
				
					rm -f $OUTDIR/"$tgtDB"_get_final_table_count.out
					$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/"$tgtDB"_get_final_table_count.sql  $OUTDIR/"$tgtDB"_get_final_table_count.out  >> $logFileName

					rm -f $TEMPDIR/"$tgtDB"_create_backup_table.sql 
					rm -f $TEMPDIR/"$tgtDB"_drop_old_backup_table.sql
					
					tail +1 $OUTDIR/"$tgtDB"_get_final_table_count.out | grep -v -w "RESULT_LINE" | while read -r line; do
					
						dbName=`echo $line | cut -f1 -d'|'`
						tabName=`echo $line | cut -f2 -d'|'`
						recordCount=`echo $line | cut -f3 -d'|'`
						
						currtabPreFix=`echo $tabName | awk '{print substr($0,1,5)}'`
						preFix=`date +%m%d`

											
						if [ "$currtabPreFix" == "U"$preFix"" ]
						then
							echo "DROP TABLE /*$ticketNo*/  $dbName.\"$tabName\";" >> $TEMPDIR/"$tgtDB"_drop_old_backup_table.sql
						else
							if [ $recordCount -gt 0 ]
							then
								tabNameLen=`expr length "$tabName"`
								if [ $tabNameLen -gt 23 ]
								then
									bkupTabName=`echo $tabName | awk '{print substr($0,1,23)}'`
								else
									bkupTabName=$tabName
								fi
							
								echo "CREATE TABLE /*$ticketNo*/ $dbName.U"$preFix"_"$bkupTabName" AS $dbName.\"$tabName\" WITH DATA AND STATS;" >> $TEMPDIR/"$tgtDB"_create_backup_table.sql 
								echo "DROP TABLE /*$ticketNo*/  $dbName.\"$tabName\";" >> $TEMPDIR/"$tgtDB"_create_backup_table.sql
								
							else
								echo "DROP TABLE /*$ticketNo*/  $dbName.\"$tabName\";" >> $TEMPDIR/"$tgtDB"_create_backup_table.sql
							fi
						fi
					done
				fi
				
				
				$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/"$tgtDB"_drop_old_backup_table.sql  $TEMPDIR/"$tgtDB"_drop_old_backup_table.out  >> $logFileName

				$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/"$tgtDB"_create_backup_table.sql  $TEMPDIR/"$tgtDB"_create_backup_table.out  >> $logFileName

				
			
			else
			
				# Cleanup the Target Database
				$SCRIPTDIR/epdba_perform_dbmaint.sh 1 $tgtDB $TDTGT $ticketNo | tee -a  $logFileName
			
			fi
		
		fi
	
	fi
	
	
	
	# STEP-8 Run the Target Defintion for Creating Tables or Views
	if [ "$exclusionValue" != "5" ] && [ "$exclusionValue" != "6" ] && [ "$exclusionValue" != "7" ] && [ "$exclusionValue" != "8" ]
	then
		startTS=`date +%Y-%m-%d\ %H:%M:%S`


		# Split the Target Defintion into multiple files if parallelism is needed
		if [ "$parallelCount" -gt 1 ]
		then
		
			#  Logic for splitting the files
			if [ "$objType" == "T" ]
			then
				$SCRIPTDIR/epdba_split_file.sh "CREATE SET TABLE" $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql $parallelCount
			fi
			if [ "$objType" == "V" ]
			then
				$SCRIPTDIR/epdba_split_file.sh "REPLACE VIEW" $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql $parallelCount
			fi
			if [ "$objType" == "P" ]
			then
				$SCRIPTDIR/epdba_split_file.sh "REPLACE PROCEDURE" $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql $parallelCount
			fi
			
			# Run the Create DDL Defintions in the Target Database (Reporting)	
			i="1"
			while [ $i -le $parallelCount ]
			do
				$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql_"$i"  $OUTDIR/"$runName"_create_"$tgtDB".out > $LOGDIR/archive/"$runName"_create_"$tgtDB"_"$i".log &
				sleep 2
				i=`expr $i + 1`
			done
			
			# Ensure that creation of Reporting Tables is complete before proceeding
			row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i "$runName"_create_"$tgtDB".sql | wc -l`
			while [ $row_cnt -gt 1 ]
			do
				sleep 10
				row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i "$runName"_create_"$tgtDB".sql | wc -l`
			done

			# Collect All the Log Files
			rm -f $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB".log
			i="1"
			while [ $i -le $parallelCount ]
			do
				if [ -f $LOGDIR/archive/"$runName"_create_"$tgtDB"_"$i".log ]
				then
					$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$runName"_create_"$tgtDB"_"$i".log $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB"_"$i".log &
					sleep 2
				fi
				rm -f $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql_"$i" 
				i=`expr $i + 1`
			done
			
			# Ensure that all Errors have been extracted before proceeding
			row_cnt=`ps -ef | grep $USER | grep -i epdba_get_logerrors | grep -i "$runName"_create_"$tgtDB" | wc -l`
			while [ $row_cnt -gt 1 ]
			do
				sleep 10
				row_cnt=`ps -ef | grep $USER | grep -i epdba_get_logerrors | grep -i "$runName"_create_"$tgtDB" | wc -l`
			done
			
			# Consolidate all Error Log Files
			if [ -f $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB"_?.log ]
			then
				cat $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB"_?.log > $LOGDIR/archive/"$runName"_create_errors_"$tgtDB".log
				rm -f $LOGDIR/archive/ "$runName"_create_"$tgtDB"_?.log
				rm -f $LOGDIR/archive/ "$runName"_create_errors_"$tgtDB"_?.log
			fi
			
		else

		
			$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $DDLBACKUPDIR/"$runName"_create_"$tgtDB".sql  $OUTDIR/"$runName"_create_"$tgtDB".out > $LOGDIR/archive/"$runName"_create_"$tgtDB".log 
			$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$runName"_create_"$tgtDB".log $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB".log 

			if [ -f $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB".log ]
			then
				cat $DDLBACKUPDIR/"$runName"_create_errors_"$tgtDB".log > $LOGDIR/archive/"$runName"_create_errors_"$tgtDB".log
				rm -f $LOGDIR/archive/ "$runName"_create_"$tgtDB".log
			fi
		
		fi
		
		
		# ALTER PROCEDURE for Procedures
		if [ "$objType" == "P" ]
		then
			$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDTGT 18 $SQLDIR/"$runName"_alter_procedure_"$tgtDB".sql  $TEMPDIR/"$dbName"_list_db_maintenace.out $logFileName 	
		fi
	
	fi
	

	
# STEP-9 Perform Validation
	
	
			# Check For Changes in Source
	echo "SELECT 'Potential Changes in Source Database $srcDB' AS INFO, COUNT(*) AS OBJECT_COUNT FROM DBC.TablesV WHERE DatabaseName='$srcDB' AND LastAlterTimeStamp >= TIMESTAMP'$startTS';" > $DDLBACKUPDIR/"$runName"_validate_object_creation_"$tgtDB".sql
	
			# Check For Changes in Target
	echo "SELECT 'Old Definition in Target Database $tgtDB' AS INFO, COUNT(*) AS OBJECT_COUNT FROM DBC.TablesV WHERE DatabaseName='$tgtDB'  AND LastAlterTimeStamp < TIMESTAMP'$startTS';" >> $DDLBACKUPDIR/"$runName"_validate_object_creation_"$tgtDB".sql
	
			# Take Count of Source and Target
	echo "SELECT DBNAME AS DATABASE_NAME,COUNT(*) AS SOURCE_OBJECT_COUNT from CLARITY_DBA_MAINT.MIGRATION_LIST WHERE ENV='$runName' AND DBNAME='$srcDB' GROUP BY 1 ORDER BY 1;" >> $DDLBACKUPDIR/"$runName"_validate_object_creation_"$tgtDB".sql
	echo "SELECT '$tgtDB' AS DATABASE_NAME, COUNT(*) AS TARGET_OBJECT_COUNT FROM DBC.TablesV WHERE DatabaseName='$tgtDB' AND LastAlterTimeStamp >= TIMESTAMP'$startTS' GROUP BY 1 ORDER BY 1;" >> $DDLBACKUPDIR/"$runName"_validate_object_creation_"$tgtDB".sql

	# Difference Between Source & Target Objects. Identify any missing objects	
	echo "	SELECT 'MISSING IN $srcDB' || ',' || TRIM(TableName) AS MISSING_OBJECTS FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$tgtDB') AND TableKind IN ('T','V','P')
	MINUS SELECT 'MISSING IN $srcDB' || ',' || TRIM(TABNAME) FROM CLARITY_DBA_MAINT.MIGRATION_LIST WHERE ENV='$runName' AND TRIM(DBNAME)IN ('$srcDB') AND TRIM(TABNAME) NOT LIKE 'UPGR_%';
	SELECT 'MISSING IN $tgtDB' || ',' || TRIM(TABNAME) AS MISSING_OBJECTS FROM CLARITY_DBA_MAINT.MIGRATION_LIST WHERE ENV='$runName' AND TRIM(DBNAME)IN ('$srcDB') AND TRIM(TABNAME) NOT LIKE 'UPGR_%'
	MINUS SELECT 'MISSING IN $tgtDB' || ',' || TRIM(TableName) AS MISSING_OBJECTS FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('$tgtDB') AND TableKind IN ('T','V','P');
	" >> $DDLBACKUPDIR/"$runName"_validate_object_creation_"$tgtDB".sql
	
	
	rm -f $OUTDIR/"$runName"_validate_object_creation_"$tgtDB".out
	$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $DDLBACKUPDIR/"$runName"_validate_object_creation_"$tgtDB".sql  $OUTDIR/"$runName"_validate_object_creation_"$tgtDB".out  > $LOGDIR/archive/"$runName"_validate_object_creation_"$tgtDB".log
	
	rm -f $LOGDIR/archive/"$runName"_validation_errors_"$tgtDB".log
	$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$runName"_validate_object_creation_"$tgtDB".log $LOGDIR/archive/"$runName"_validation_errors_"$tgtDB".log

	
	
	if [  "$userViewFlag" == "Y" ]
	then
	
		echo ".EXPORT RESET;" > $DDLBACKUPDIR/"$runName"_validate_runtime_errors_"$tgtDB".sql
		echo ".EXPORT REPORT FILE = $DDLBACKUPDIR/"$runName"_validate_userview.sql;" >> $DDLBACKUPDIR/"$runName"_validate_runtime_errors_"$tgtDB".sql
		
		echo "SELECT 'SELECT COUNT(1) FROM $tgtDB.\"' || TRIM(TableName) || '\";' " >> $DDLBACKUPDIR/"$runName"_validate_runtime_errors_"$tgtDB".sql
		echo "FROM DBC.TablesV WHERE TableKind='V' AND TRIM(DatabaseName)='$tgtDB';" >> $DDLBACKUPDIR/"$runName"_validate_runtime_errors_"$tgtDB".sql
		
		$SCRIPTDIR/epdba_runSQLFile.sh $TDTARGET $DDLBACKUPDIR/"$runName"_validate_runtime_errors_"$tgtDB".sql  $OUTDIR/"$runName"_validate_runtime_errors_"$tgtDB".out  > $LOGDIR/archive/"$runName"_validate_runtime_errors_"$tgtDB".log

		sed '1,2d' $DDLBACKUPDIR/"$runName"_validate_userview_"$tgtDB".sql > $DDLBACKUPDIR/"$runName"_validate_userview_"$tgtDB".tmp
		mv $DDLBACKUPDIR/"$runName"_validate_userview_"$tgtDB".tmp $DDLBACKUPDIR/"$runName"_validate_userview_"$tgtDB".sql
		
		$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDTARGET 24 $DDLBACKUPDIR "$runName"_validate_userview_"$tgtDB".sql $TEMPDIR/"$dbName"_validate_userview_"$tgtDB".out $LOGDIR/archive/"$runName"_validate_userview.log

		$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$runName"_validate_userview_"$tgtDB".log $LOGDIR/archive/"$runName"_validation_errors_"$tgtDB".log
		
	fi
	

	
	
	