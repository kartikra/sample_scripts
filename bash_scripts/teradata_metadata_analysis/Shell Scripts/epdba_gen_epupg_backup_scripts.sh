#!/usr/bin/ksh

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

	echo "---------------------------------------------------------------" >> $logFileName
	echo "---------------- Preparing Backup Jobs    ---------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	option=$1

	echo "--------------- Option Selected = $option ---------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName


	runId="1"   
	ticketNo="CRQ000000205738"             #  Get From Remedy or Ask ETL team
	regionProfile="TESTNW"                 #  Look at Db Change list and detrrmine which profile to use
	region="NW"			       #  Region of the upgrade 
	relName="nw0612"                       #   Region + Cutover Date
	prodbackupDB="HCCLNW_12_0516"      #  Get this from Tom



		# Create these files from db change list under /HOME/EDWDBA/dbmig/Release/

	backupList="nw0612_backup_list_vf.txt" 
	stagingList="nw0612_staging_list_vf.txt"
	dbChgList="nw0612_chglist_vf.txt"
	

	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
        fi


#-----------------------------------------------------------------------------------------------------------------------------------------------------


	# Option-0  Use this option for taking DDL Backup

	if [ "$option" == "0" ]
	then

		# Backup Staging Database

		if [  -f "$DDLBACKUPDIR/"$relName"_backup_export_stagingDB_ddl.sql" ]
		then
			echo " File "$relName"_backup_export_stagingDB_ddl.sql Already Exists in DDL Backup Directory " >> $logFileName
		else
			$SCRIPTDIR/epdba_export_ddl.sh $TDPROD $prodStgDB "$relName"_backup_export_stagingDB_ddl.sql &
			sleep 5
		fi


		# Backup Reporting Database
		if [  -f "$DDLBACKUPDIR/"$relName"_backup_export_reportingDB_ddl.sql" ]
		then
			echo " File "$relName"_backup_export_reportingDB_ddl.sql Already Exists in DDL Backup Directory " >> $logFileName
		else
			$SCRIPTDIR/epdba_export_ddl.sh $TDPROD $prodReportDB "$relName"_backup_export_reportingDB_ddl.sql &
			sleep 5
		fi


		# Backup  View
		if [ -f "$DDLBACKUPDIR/"$relName"_backup_export_View_ddl.sql" ]
		then
			echo " File "$relName"_backup_export_View_ddl.sql Already Exists in DDL Backup Directory " >> $logFileName
		else
			$SCRIPTDIR/epdba_export_ddl.sh $TDPROD $prodView "$relName"_backup_export_View_ddl.sql &
			sleep 5
		fi


		# Backup User View
		if [ -f "$DDLBACKUPDIR/"$relName"_backup_export_userView_ddl.sql" ]
		then
			echo " File "$relName"_backup_export_userView_ddl.sql Already Exists in DDL Backup Directory " >> $logFileName
		else
			$SCRIPTDIR/epdba_export_ddl.sh $TDPROD $prodUserView "$relName"_backup_export_userView_ddl.sql &
		fi


	fi



	# Option-1  Use this option for cleaning up the Temp and backup databases

	if [ "$option" == "1" ]
	then
		# Drop all tables from temp database
		$SCRIPTDIR/epdba_perform_dbmaint.sh 1  HCCL"$region"_UPG_AK_% $TDPROD $ticketNo

		# Drop all tables from backup database
		#$SCRIPTDIR/epdba_perform_dbmaint.sh 1  $prodbackupDB $TDPROD $ticketNo
	fi





#-----------------------------------------------------------------------------------------------------------------------------------------------------


	# 2  Create scripts for copying tables from Production to Backup. 
	#   Tables not found are moved to a seperate file for analysis(manual step)

	if [ "$option" == "2" ]
	then

		# Move all objects that are not found to a different file
		echo "Backup Tables Not Found - " > $DIR/"$relName"_notfound_"$backupList"

		# Cleanup old file
		rm -f $DIR/"$relName"_final_"$backupList"
		rm -f $DIR/"$relName"_all_"$backupList"

		# Start script creation

		cat $DIR/$backupList | sort | uniq | while read -r name ; do

			echo "SELECT '$prodbackupDB|' || TRIM(databaseName) || '|' || TRIM(TableName) || '|' || TRIM(TableKind) FROM dbc.TablesV WHERE TableName='$name' AND DatabaseName IN ('$prodView','$prodUserView','$prodReportDB','$prodStgDB');" > $TEMPDIR/"$relName"_find_backup_table.sql
			
			$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_find_backup_table.sql $TEMPDIR/"$relName"_find_backup_table.out | tee -a $logFileName
			sed '1,2d'  $TEMPDIR/"$relName"_find_backup_table.out > $TEMPDIR/"$relName"_results_find_backup_table.out
			count1=`cat $TEMPDIR/"$relName"_results_find_backup_table.out | wc -l`

			if [ $count1 -ne 0 ]
			then
				count2=`cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep $prodReportDB | wc -l`
				if [ $count2 -ne 0 ]
				then
					cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep $prodReportDB >> $DIR/"$relName"_final_"$backupList"
				else
					count3=`cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep "$prodUserView|" | wc -l`

					if [ $count3 -ne 0 ]
					then
						cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep "$prodUserView|" >> $DIR/"$relName"_final_"$backupList"
					else
						count4=`cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep "$prodView|" | wc -l`
						if [ $count4 -ne 0 ]
						then
							cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep "$prodView|" >> $DIR/"$relName"_final_"$backupList"
						else
							cat $TEMPDIR/"$relName"_results_find_backup_table.out | grep $prodStgDB >> $DIR/"$relName"_final_"$backupList"
						fi
					fi
				fi

				cat $TEMPDIR/"$relName"_results_find_backup_table.out >> $DIR/"$relName"_all_"$backupList"

			else
				echo $name >> $DIR/"$relName"_notfound_"$backupList"

			fi

		done

	fi



	# 3  Generate All the scripts required for taking backup
        #   CREATE TABLE AS	for taking initial backup
        #   DELETE, ALTER TABLE for setting block level compression
        #   INSERT SCRIPTS 	for loading data in Backup Tables

	if [ "$option" == "3" ]
	then


	      	rm -f $SQLDIR/"$relName"_backup_copy_prod_tables.sql
		rm -f $SQLDIR/"$relName"_backup_set_blocklevel_compression.sql
		rm -f $SQLDIR/"$relName"_backup_move_prod_tables.sql

		echo "SET QUERY_BAND='BlockCompression=YES;' FOR SESSION;" > $SQLDIR/"$relName"_backup_move_data.sql



		cat $DIR/"$relName"_final_"$backupList" | sort | uniq | while read -r line ; do 

			backupDB=`echo $line | cut -f1 -d'|'`
			sourceDB=`echo $line | cut -f2 -d'|'`
			sourceTable=`echo $line | cut -f3 -d'|'`
			sourceType=`echo $line | cut -f4 -d'|'`
			if [ "$sourceType" == "T" ]
			then
				echo "CREATE TABLE /*$ticketNo*/ $backupDB.\"$sourceTable\" AS $sourceDB.\"$sourceTable\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_backup_move_prod_tables.sql
			fi

			if [ "$sourceType" == "V" ]
			then
				echo "CREATE MULTISET TABLE /*$ticketNo*/ $backupDB.\"$sourceTable\" AS (SELECT A.* FROM $sourceDB.\"$sourceTable\" A) WITH DATA;" >> $SQLDIR/"$relName"_backup_move_prod_tables.sql
			fi


			echo "DELETE FROM /*$ticketNo*/ $backupDB.\"$sourceTable\" ALL;" >> $SQLDIR/"$relName"_backup_set_blocklevel_compression.sql
			echo "ALTER TABLE /*$ticketNo*/ $backupDB.\"$sourceTable\",NO FALLBACK, BLOCKCOMPRESSION = MANUAL;" >> $SQLDIR/"$relName"_backup_set_blocklevel_compression.sql

			echo "INSERT INTO /*$ticketNo*/ $backupDB.\"$sourceTable\" SELECT * FROM $sourceDB.\"$sourceTable\";" >> $SQLDIR/"$relName"_backup_move_data.sql


		done

		echo "SELECT 'TABLE SIZE OF : ' || TableName AS TABLESIZE , ( SUM(CurrentPerm) / 1000000 ) AS \"CurrentPerm in MB\"  FROM DBC.tableSize WHERE DatabaseName='$prodbackupDB' GROUP BY 1;" >> $SQLDIR/"$relName"_backup_move_prod_tables.sql

		echo "SELECT 'TABLE SIZE OF : ' || TableName AS TABLESIZE , ( SUM(CurrentPerm) / 1000000 ) AS \"CurrentPerm in MB\"  FROM DBC.tableSize WHERE DatabaseName='$prodbackupDB' GROUP BY 1;" >> $SQLDIR/"$relName"_backup_move_data.sql

	fi



	# 3.1 Execute Backup. File created using option-3

	if [ "$option" == "3.1" ]
	then
		rm -f $OUTDIR/"$relName"_backup_move_prod_tables.out
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_move_prod_tables.sql $OUTDIR/"$relName"_backup_move_prod_tables.out | tee -a $logFileName
	fi



	# 3.2 Execute DELETE, ALTER TABLE. File created using option-3

	if [ "$option" == "3.2" ]
	then
		rm -f $OUTDIR/"$relName"_backup_copy_prod_tables.out
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_set_blocklevel_compression.sql $OUTDIR/"$relName"_backup_set_blocklevel_compression.out | tee -a $logFileName
	fi



	# 3.3 Execute INSERT SCRIPTS. File created using option-3

	if [ "$option" == "3.3" ]
	then
		echo "SELECT 'AFTER COMPRESSION TABLE SIZE OF : ' || TableName AS TABLESIZE , ( SUM(CurrentPerm) / 1000000 ) AS \"CurrentPerm in MB\"  FROM DBC.tableSize WHERE DatabaseName='$prodbackupDB' GROUP BY 1;" >> $SQLDIR/"$relName"_backup_move_data.sql

		rm -f $OUTDIR/"$relName"_backup_move_data.out
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_move_data.sql $OUTDIR/"$relName"_backup_move_data.out | tee -a $logFileName
	fi


	# Validation of Table Size Before and After Compression



#-----------------------------------------------------------------------------------------------------------------------------------------------------


	# 4 Copy Staging Table Scripts from DEV to TEMP STAGING in PROD. Generatae Scripts for Dev to PROD as well.
	
	if [ "$option" == "4" ]
	then

		rm -f $SQLDIR/"$relName"_backup_create_prod_staging.sql
		rm -f $SQLDIR/"$relName"_backup_drop_prod_staging.sql


		cat $DIR/$stagingList | sort | uniq | while read -r line ; do 
			
			action=`echo $line | cut -f1 -d'|'`
			sourceTable=`echo $line | cut -f2 -d'|'`
			sourceDB=`echo $line | cut -f3 -d'|'`
			finalProdDB=`echo $line | cut -f4 -d'|'`


			if [ "$action" == "ADD" ]
			then
				echo "DROP TABLE $backupDB.\"$sourceTable\";"  >> $TEMPDIR/"$relName"_backup_drop_temp_prod.sql

				rm -f $TEMPDIR/"$relName"_backup_create_temp_prod.out
				echo "SHOW TABLE $sourceDB.\"$sourceTable\";"  > $TEMPDIR/"$relName"_backup_create_temp_prod.sql
				$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" $TEMPDIR/"$relName"_backup_create_temp_prod.sql $TEMPDIR/"$relName"_backup_create_temp_prod.out | tee -a $logFileName

				sed  -e 's/'$sourceDB'/'$finalProdDB'/g' $TEMPDIR/"$relName"_backup_create_temp_prod.out >> $SQLDIR/"$relName"_backup_create_prod_staging.sql

			fi
				
			echo "DROP TABLE $finalProdDB.\"$sourceTable\";" >> $SQLDIR/"$relName"_backup_drop_prod_staging.sql

		done


	fi


	if [ "$option" == "4.1" ]
	then

		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_drop_prod_staging.sql $TEMPDIR/"$relName"_backup_create_prod_temp_staging.out | tee -a $logFileName	

		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_create_prod_staging.sql $TEMPDIR/"$relName"_backup_create_prod_temp_staging.out | tee -a $logFileName	
	fi





#-----------------------------------------------------------------------------------------------------------------------------------------------------



	# 5 Create Scripts for VIEW REFRESH. Point the  and User views in PROD to the backup database.

	if [ "$option" == "5" ]
	then
		rm -f $SQLDIR/"$relName"_backup_rename_prod_userViews.sql
		rm -f $SQLDIR/"$relName"_backup_rename_prod_Views.sql

		echo "SET QUERY_BAND='BlockCompression=YES;' FOR SESSION;" > $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql


		cat $DIR/"$relName"_final_"$backupList"  | sort | uniq | while read -r line ; do 

			backupDB=`echo $line | cut -f1 -d'|'`
			sourceDB=`echo $line | cut -f2 -d'|'`
			sourceTable=`echo $line | cut -f3 -d'|'`
			sourceType=`echo $line | cut -f4 -d'|'`
			newTable=UPGR_"$sourceTable"



			if [ "$sourceType" == "T" ] && [ "$sourceDB" == "$prodReportDB" ]
			then

				echo "SHOW VIEW  $prodView.\"$sourceTable\" ;" > $TEMPDIR/"$relName"_backup_move_prod_Views.sql
				rm -f $TEMPDIR/"$relName"_backup_move_prod_Views.out
				$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_backup_move_prod_Views.sql $TEMPDIR/"$relName"_backup_move_prod_Views.out | tee -a $logFileName
				sed -e 's/VIEW\ /VIEW\ \/\*'$ticketNo'\*\//'  -e 's/'\"$sourceDB\"'/'$backupDB'/g'  -e 's/'$sourceDB'/'$backupDB'/g'  -e 's/'$prodView.$sourceTable'/'$prodView.$newTable'/g' -e 's/'$prodView.\"$sourceTable\"'/'$prodView.\"$newTable\"'/g'  -e 's/'\"$prodView\".\"$sourceTable\"'/'$prodView.\"$newTable\"'/g' $TEMPDIR/"$relName"_backup_move_prod_Views.out >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql
				
				checkCount1=`cat $TEMPDIR/"$relName"_backup_move_prod_Views.out | wc -l`

				if [ $checkCount1 -eq 0 ]
				then

					echo " REPLACE VIEW  /*$ticketNo*/ $prodView."$newTable" " >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql					
					echo " AS LOCKING $backupDB."$sourceTable" FOR ACCESS " >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql
					echo " SELECT * FROM $backupDB."$sourceTable";" >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql

				fi


					echo "SHOW VIEW  $prodUserView.\"$sourceTable\" ;" > $TEMPDIR/"$relName"_backup_move_prod_userViews.sql
					rm -f $TEMPDIR/"$relName"_backup_move_prod_userViews.out
					$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_backup_move_prod_userViews.sql $TEMPDIR/"$relName"_backup_move_prod_userViews.out | tee -a $logFileName

					checkCount2=`cat $TEMPDIR/"$relName"_backup_move_prod_userViews.out | wc -l`

					if [ $checkCount2 -ne 0 ]
					then
						sed -e 's/VIEW\ /VIEW\ \/\*'$ticketNo'\*\//' -e 's/'$prodView.$sourceTable'/'$prodView.$newTable'/g'   -e 's/'$prodView.\"$sourceTable\"'/'$prodView.\"$newTable\"'/g'  -e 's/'\"$prodView\".\"$sourceTable\"'/'$prodView.\"$newTable\"'/g' -e 's/'$prodUserView.$sourceTable'/'$prodUserView.$newTable'/g'  -e 's/'\"$prodUserView\".\"$sourceTable\"'/'$prodUserView.\"$newTable\"'/g'  -e 's/'$prodUserView.\"$sourceTable\"'/'$prodUserView.\"$newTable\"'/g' $TEMPDIR/"$relName"_backup_move_prod_userViews.out >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql


						# Checking to see if User View has more than 1 table in a UNION or JOIN

						complexCount=`cat $TEMPDIR/"$relName"_backup_move_prod_userViews.out | tr '[a-z]' '[A-Z]' | egrep -h 'UNION |JOIN ' | wc -l`
						if [ $complexCount -ne 0 ]
						then
							echo "User View :  $sourceTable " >> $logFileName

							echo "DELETE FROM /*$ticketNo*/ $backupDB.$sourceTable ALL;"  >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " "  >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
	
							echo "INSERT INTO /*$ticketNo*/ $backupDB.$sourceTable"  >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							cat $TEMPDIR/"$relName"_backup_move_prod_userViews.out  >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo "-------------------------------------------------"  >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql


							echo " REPLACE VIEW  /*$ticketNo*/ $prodView."$newTable" " >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " AS LOCKING $backupDB."$sourceTable" FOR ACCESS " >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " SELECT * FROM $backupDB."$sourceTable";" >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " "  >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " REPLACE VIEW  /*$ticketNo*/ $prodUserView."$newTable" AS " >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " LOCKING $prodView."$newTable" FOR ACCESS " >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql
							echo " SELECT * FROM $prodView."$newTable";" >> $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql


						fi
					else

						echo " REPLACE VIEW  /*$ticketNo*/ $prodUserView."$newTable" AS " >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql	
						echo " LOCKING $prodView."$newTable" FOR ACCESS " >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql	
						echo " SELECT * FROM $prodView."$newTable";" >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql	

					fi


			else

				echo " REPLACE VIEW  /*$ticketNo*/ $prodView."$newTable" " >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql					
				echo " AS LOCKING $backupDB."$sourceTable" FOR ACCESS " >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql
				echo " SELECT * FROM $backupDB."$sourceTable";" >> $SQLDIR/"$relName"_backup_rename_prod_Views.sql


				echo " REPLACE VIEW  /*$ticketNo*/ $prodUserView."$newTable" AS " >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql	
				echo " LOCKING $prodView."$newTable" FOR ACCESS " >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql	
				echo " SELECT * FROM $prodView."$newTable";" >> $SQLDIR/"$relName"_backup_rename_prod_userViews.sql	

			fi


		done

	fi


	# 5.1  Execute Scripts for VIEW REFRESH. File Created in 5

	if [ "$option" == "5.1" ]
	then

		# Cleanup All UPGR_ Views in  VIEW and USER VIEW
		
		echo " SELECT 'DROP VIEW /*$ticketNo*/'  || TRIM(DatabaseName)  || '.' || TRIM( tableName) || ';' AS SQL_QUERY  from DBC.TablesV " > $TEMPDIR/"$relName"_backup_cleanup_upgrViews.sql
		echo " WHERE DatabaseName IN ('$prodView','$prodUserView') AND TableName LIKE 'UPGR_%' " >> $TEMPDIR/"$relName"_backup_cleanup_upgrViews.sql
		
		rm -f $TEMPDIR/"$relName"_backup_cleanup_upgrViews.out

		#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_backup_cleanup_upgrViews.sql $TEMPDIR/"$relName"_backup_cleanup_upgrViews.out | tee -a $logFileName
		#sed '1,2d'  $TEMPDIR/"$relName"_backup_cleanup_upgrViews.out > $TEMPDIR/"$relName"_backup_final_cleanup_upgrViews.sql
		#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_backup_final_cleanup_upgrViews.sql $TEMPDIR/"$relName"_backup_final_cleanup_upgrViews.out | tee -a $logFileName



		#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_move_prod_Views.sql $TEMPDIR/"$relName"_backup_rename_prod_Views.out | tee -a $logFileName
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/final_"$relName"_backup_move_prod_userViews.sql $TEMPDIR/"$relName"_backup_rename_prod_userViews.out | tee -a $logFileName
		#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/"$relName"_backup_rename_prod_complex_userViews.sql $TEMPDIR/"$relName"_backup_rename_prod_complex_userViews.out | tee -a $logFileName

	fi




#-----------------------------------------------------------------------------------------------------------------------------------------------------




	# 6.1  Validation of Counts of Prod Table and Backup Table

	if [ "$option" == "6.1" ]
	then

		cat $DIR/"$relName"_final_"$backupList" | cut -f2,3 -d'|' > $AUDITDIR/"$relName"_source_backup.dat
		$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_backup.dat $TDPROD $ticketNo $runId 101


		cat $DIR/"$relName"_final_"$backupList" | cut -f1,3 -d'|' > $AUDITDIR/"$relName"_target_backup.dat
		$SCRIPTDIR/epdba_load_audit.sh "$relName"_target_backup.dat $TDPROD $ticketNo $runId 201

	fi


	# 6.2
	# Validation-1  No of UPGR Views Created today
	# Count of UPGR_ and Original view must match
	
	if [ "$option" == "6.2" ]
	then

		cat $DIR/"$relName"_all_"$backupList" | grep $prodView\| | cut -f2,3 -d'|' > $AUDITDIR/"$relName"_source_view.dat
		$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_backup.dat $TDPROD $ticketNo $runId 102


		cat $DIR/"$relName"_all_"$backupList" | grep  $prodUserView\| | cut -f2,3 -d'|' > $AUDITDIR/"$relName"_source_userview.dat
		$SCRIPTDIR/epdba_load_audit.sh "$relName"_target_backup.dat $TDPROD $ticketNo $runId 103



		sed 's/\|/\|UPGR_/g' $AUDITDIR/"$relName"_source_view.dat > $AUDITDIR/"$relName"_target_view.dat
		$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_backup.dat $TDPROD $ticketNo $runId 202


		sed 's/\|/\|UPGR_/g' $AUDITDIR/"$relName"_source_userview.dat > $AUDITDIR/"$relName"_target_userview.dat
		$SCRIPTDIR/epdba_load_audit.sh "$relName"_target_backup.dat $TDPROD $ticketNo $runId 203


	fi



	echo "---------------------------------------------------------------" >> $logFileName
	echo "---------------- Completed Backup Scripts ---------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

