#!/usr/bin/ksh

					# MY To Do List
#  UPGR_ check if original not being replaced
#  Count of views with lastaltertimestamp and not= UPGR_ must be 0
#  Logic for Mat View Impact


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
	echo "--------------- Preparing Migration Scripts -------------------" >> $logFileName
	echo "------------ Started at `date +%Y-%m-%d\ %H:%M:%S` ------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	

	relName=$1
	ticketNo=$2
	regionProfile=$3
	dbChgList=$4
	stagingList=$5
	customViewPurpose=$6

	
	
	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	if [ -z "$relName" ] || [ -z "$ticketNo" ] || [ -z "$regionProfile" ] || [ -z "$dbChgList" ] || [ -z "$stagingList" ] 
	then
		echo "Not All Mandatory Parameters available.. Aborting Script"
		exit 901
	fi
	
	
	if [ -f "$SQLDIR/archive/"$relName"/runid.profile" ]
	then
		chmod 775 $SQLDIR/archive/"$relName"/runid.profile
		. $SQLDIR/archive/"$relName"/runid.profile
	else
		echo "Profile File for Run Id is Missing"
		exit 904
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

		
create_tpf_storedProc()
{

	table=$1
	inputStgDeployment=$2
	subDeployStgDB=$3
	subTpfReportDB=$4
	subTpfDeployStgDB=$5
	traceDeployId=$6
	subDeployId=$7
	
	if [ -s $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat ]
	then
	
		storeProcDB=`echo $subDeployStgDB | sed -e 's/_S/_SP/g'`

		if [ $region == "NC" ]
		then
			cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$storeProcDB'/g' -e 's/'MY_TABLE'/'$table'/g' \
			-e 's/'MY_OUTDDL_FILE'/'get_tpf_storeproc\.dat'/g' -e 's/'MY_OBJECT'/'PROCEDURE'/g'  > $TEMPDIR/get_tpf_storeproc.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" "$TEMPDIR/get_tpf_storeproc.sql" "$TEMPDIR/get_tpf_storeproc.dat" | tee -a  $logFileName
			if [ -s $TEMPDIR/get_tpf_storeproc.dat ]
			then
				cat $TEMPDIR/get_tpf_storeproc.dat > $TEMPDIR/tpf_stg_storeproc.dat
			else
				# Create code for Loading TPF Staging
				echo "REPLACE PROCEDURE $storeProcDB.$table()" > $TEMPDIR/tpf_stg_storeproc.dat
				echo "BEGIN " >> $TEMPDIR/tpf_stg_storeproc.dat
				cat $SQLDIR/accdba_tpf_stg_storeproc.sql | sed -e 's/'MY_TPF_STG'/'$subTpfDeployStgDB'/g' -e 's/'MY_STAGE_DB'/'$subDeployStgDB'/g' -e 's/'MY_USER_DB'/'$prodUserView'/g' >> $TEMPDIR/tpf_stg_storeproc.dat
			fi
			echo "END;" >> $TEMPDIR/tpf_stg_storeproc.dat
			echo "ALTER PROCEDURE $storeProcDB."$table" COMPILE;" >> $TEMPDIR/tpf_stg_storeproc.dat
		fi

		if [ $region == "SC" ]
		then
			cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$storeProcDB'/g' -e 's/'MY_TABLE'/'$table'/g' \
			-e 's/'MY_OUTDDL_FILE'/'get_tpf_storeproc\.dat'/g' -e 's/'MY_OBJECT'/'PROCEDURE'/g'  > $TEMPDIR/get_tpf_storeproc.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" "$TEMPDIR/get_tpf_storeproc.sql" "$TEMPDIR/get_tpf_storeproc.dat" | tee -a  $logFileName
			
			if [ -s $TEMPDIR/get_tpf_storeproc.dat ]
			then
				lineNo=`cat $TEMPDIR/get_tpf_storeproc.dat | grep -i -w -n "TRANSACTION" | grep -i -w "BEGIN"  | cut -f1 -d ':'`
				# get everything till the line BEGIN TRANSACTION	
				head -$lineNo $TEMPDIR/get_tpf_storeproc.dat > $TEMPDIR/tpf_stg_storeproc.dat
			else
				# Create code for Loading TPF Staging
				echo "REPLACE PROCEDURE $storeProcDB.$table()" > $TEMPDIR/tpf_stg_storeproc.dat
				echo "BEGIN " >> $TEMPDIR/tpf_stg_storeproc.dat
				cat $SQLDIR/accdba_tpf_stg_storeproc.sql | sed -e 's/'MY_TPF_STG'/'$subTpfDeployStgDB'/g' -e 's/'MY_STAGE_DB'/'$subDeployStgDB'/g' -e 's/'MY_USER_DB'/'$prodUserView'/g' >> $TEMPDIR/tpf_stg_storeproc.dat
				echo "BEGIN	TRANSACTION;" >> $TEMPDIR/tpf_stg_storeproc.dat
			fi
		fi
		
				
		if [ $region == "NC" ]
		then
			echo "REPLACE PROCEDURE $storeProcDB.B_"$table"()" > $TEMPDIR/tpf_rpt_storeproc.dat
			echo "BEGIN	" >> $TEMPDIR/tpf_rpt_storeproc.dat
			echo "BEGIN	TRANSACTION;" >> $TEMPDIR/tpf_rpt_storeproc.dat
		fi


		cat $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat >> $TEMPDIR/tpf_rpt_storeproc.dat

		perl -pi -e 's/'$inputStgDeployment'/'$subDeployStgDB'/gi'  $TEMPDIR/tpf_rpt_storeproc.dat 
		perl -pi -e 's/'$devReportDB'/'$subTpfReportDB'/gi'  $TEMPDIR/tpf_rpt_storeproc.dat 
		perl -pi -e 's/'$traceDeployId'/'$subDeployId'/gi'  $TEMPDIR/tpf_rpt_storeproc.dat 

		grep -n -w -i "$table" $TEMPDIR/tpf_rpt_storeproc.dat  | grep -v "$subTpfReportDB" > $TEMPDIR/tpf_rpt_storeproc_line.dat

		lastLineNo=`tail -1 $TEMPDIR/tpf_rpt_storeproc_line.dat | cut -f1 -d':'`
		
		# # Remove " from the last line
		# sed  ''$lastLineNo's/\"//g'  $TEMPDIR/tpf_rpt_storeproc.dat > $TEMPDIR/tpf_rpt_storeproc.tmp
		# mv $TEMPDIR/tpf_rpt_storeproc.tmp $TEMPDIR/tpf_rpt_storeproc.dat
		
		#perl -pi -e 's/'$subDeployStgDB'\.\ /'$subDeployStgDB'\./gi'  $TEMPDIR/tpf_rpt_storeproc.dat 
		#perl -pi -e 's/'$subDeployStgDB'\.\'\t'/'$subDeployStgDB'\./gi' $TEMPDIR/tpf_rpt_storeproc.dat

		perl -pi -e 's/\"'$subDeployStgDB'\"\.\"'$table'\"/\"'$subTpfDeployStgDB'\"\.\"'$table'\"/gi'  $TEMPDIR/tpf_rpt_storeproc.dat 

		echo "END TRANSACTION;" >> $TEMPDIR/tpf_rpt_storeproc.dat
		echo "END;" >> $TEMPDIR/tpf_rpt_storeproc.dat
		
		if [ $region == "NC" ]
		then
			echo "ALTER PROCEDURE $storeProcDB.B_"$table" COMPILE;" >> $TEMPDIR/tpf_rpt_storeproc.dat
		fi
		if [ $region == "SC" ]
		then
			echo "ALTER PROCEDURE $storeProcDB."$table" COMPILE;" >> $TEMPDIR/tpf_rpt_storeproc.dat
		fi
		
		
		# Clecnup OLD CRQ Numbers from Stg
		mv $TEMPDIR/tpf_stg_storeproc.dat $SQLDIR/tpf_stg_storeproc.sql
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR tpf_stg_storeproc.sql 
		sed -e 's/[Pp][Rr][Oo][Cc][Ee][Dd][Uu][Rr][Ee]\ /PROCEDURE\ \/\*'$ticketNo'\*\//' $SQLDIR/tpf_stg_storeproc.sql >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		rm -f $SQLDIR/tpf_stg_storeproc.sql
		
		# Clecnup OLD CRQ Numbers from Rpt
		mv $TEMPDIR/tpf_rpt_storeproc.dat $SQLDIR/tpf_rpt_storeproc.sql
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR tpf_rpt_storeproc.sql
		sed -e 's/[Pp][Rr][Oo][Cc][Ee][Dd][Uu][Rr][Ee]\ /PROCEDURE\ \/\*'$ticketNo'\*\//' $SQLDIR/tpf_rpt_storeproc.sql >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		rm -f $SQLDIR/tpf_rpt_storeproc.sql
		
		echo "" >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		echo "" >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		
	fi
	
}		

#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-3 Entry in Metadata Table

	echo "---------------------------------------------------------------" >> $logFileName
	echo "Creating Entry in Metadata Table ... " >> $logFileName
	rm -f $OUTDIR/"$relName"_migration_analysis.out

	# Load DB Change List in metadata tables
	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_CHG_LIST  -d $DIR/$dbChgList -l $logFileName 

	sed -e 's/'MY_RUN_ID'/'$runId'/g' -e 's/'MY_REG'/'$region'/g' -e 's/'MY_NUID'/'$USER'/g' -e 's/'MY_REL_NAME'/'$relName'/g' \
	-e 's/'MY_REPORT_DB'/'$prodReportDB'/g' -e 's/'MY_WITS_DB'/'$devReportDB'/g'  -e 's/'MY_USER_VIEW'/'$prodUserView'/g'  \
	-e 's/'MY__VIEW1'/'$prodMatView'/g' -e 's/'MY__VIEW2'/'$prodKPBIView'/g' -e 's/'MY__VIEW'/'$prodView'/g'  \
	-e 's/'MY_CALC_DB'/'$prodCalcReportDB0'/g' -e 's/'MY_MATVIEW_DB1'/'$prodMatReportDB'/g' -e 's/'MY_MATVIEW_DB2'/'$prodKPBIReportDB'/g' \
	-e 's/'MY_WITS_A_DB'/'$devCalcReportDB1'/g' -e 's/'MY_WITS_B_DB'/'$devCalcReportDB2'/g' -e 's/'MY_WITS_C_DB'/'$devCalcReportDB3'/g' \
	-e 's/'MY_WITS_D_DB'/'$devCalcReportDB4'/g' -e 's/'MY_WITS_E_DB'/'$devCalcReportDB5'/g' -e 's/'MY_WITS_F_DB'/'$devCalcReportDB6'/g' -e 's/'MY_WITS_G_DB'/'$devCalcReportDB7'/g' \
	-e 's/'MY_CALC_A_DB'/'$prodCalcReportDB1'/g' -e 's/'MY_CALC_B_DB'/'$prodCalcReportDB2'/g' -e 's/'MY_CALC_C_DB'/'$prodCalcReportDB3'/g' \
	-e 's/'MY_CALC_D_DB'/'$prodCalcReportDB4'/g' -e 's/'MY_CALC_E_DB'/'$prodCalcReportDB5'/g' -e 's/'MY_CALC_F_DB'/'$prodCalcReportDB6'/g' \
	-e 's/'MY_CALC_G_DB'/'$prodCalcReportDB7'/g' $SQLDIR/accdba_migration_analysis.sql > $TEMPDIR/"$relName"_accdba_migration_analysis.sql
	
	# Generate Migration Analysis File
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_accdba_migration_analysis.sql $OUTDIR/"$relName"_migration_analysis.out | tee -a  $logFileName

	echo "Entry has been made in Metadata Table !! " >> $logFileName
	rm -f $SQLDIR/"$relName"_script_generated_exceptions.sql

	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-4 Creation of Temporary Tables

	echo "---------------------------------------------------------------" >> $logFileName
	echo "Creating Temp Table Strcutures ... " >> $logFileName	

	$SCRIPTDIR/epdba_upgrade_create_migration_scripts_for_temptables.sh "$relName" "$ticketNo" "$regionProfile" "$dbChgList" "$stagingList" &
	
	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-5 Get  View DDL from WITS. Convert it to Prod Definition

	echo "---------------------------------------------------------------" >> $logFileName
	echo "Copying  DDL for  Views from WITS to PROD ... " >> $logFileName

	$SCRIPTDIR/epdba_upgrade_create_migration_scripts_for_view.sh "$relName" "$ticketNo" "$regionProfile" "$dbChgList" "$stagingList" &

#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-6 Get User View DDL from WITS. Convert it to Prod Definition

	echo "---------------------------------------------------------------" >> $logFileName
	echo "Copying  DDL for USER Views from WITS to PROD ... " >> $logFileName

	$SCRIPTDIR/epdba_upgrade_create_migration_scripts_for_userview.sh "$relName" "$ticketNo" "$regionProfile" "$dbChgList" "$stagingList" "$customViewPurpose" &


#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-7 Generation of Conversion Scripts. WITS to PROD (Temp Table)

	echo "Creating Conversion Scripts ... " >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	$SCRIPTDIR/epdba_upgrade_create_migration_scripts_for_data_conversion.sh "$relName" "$ticketNo" "$regionProfile" "$dbChgList" "$stagingList" &
	
	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_BACKUP_REPLACE_LIST  -d $AUDITDIR/"$relName"_compare_migration_count.dat -l $logFileName 

	
# ----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-8 Create Stats Collection Scripts. Collecting Stats on Temp Database in PROD

	echo "Creating Stats Collection Scripts ... " >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	rm -f $SQLDIR/"$relName"_migrate_stats_exist.sql
	rm -f $SQLDIR/"$relName"_migrate_stats_new.sql
	rm -f $SQLDIR/"$relName"_post_cutover_collect_stats_new_tables.sql
	rm -f $SQLDIR/"$relName"_post_cutover_collect_stats_existing_tables.sql

	rm -f $TEMPDIR/get_new_stats.sql
	rm -f $TEMPDIR/get_new_stats.out
	rm -f $TEMPDIR/get_existing_stats.sql
	rm -f $TEMPDIR/get_existing_stats.out	
	
	rm -f $TEMPDIR/create_prod_new_stats.sql
	rm -f $TEMPDIR/create_prod_new_stats.out
	rm -f $TEMPDIR/create_prod_exisiting_stats.sql
	rm -f $TEMPDIR/create_prod_existing_stats.out	
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line ; do      
		
		type=`echo $line | cut -f1 -d'|'`
		witsDB=`echo $line | cut -f2 -d'|'`
		tempDB=`echo $line | cut -f3 -d'|'`
		prodDB=`echo $line | cut -f4 -d'|'`
		table=`echo $line | cut -f5 -d'|'`


		PIChangeInd=`echo $tempDB | grep -i "PI_Change" | wc -l`
		
		echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (1,'$prodDB','$table','$tempDB',line1);" >> $TEMPDIR/getstats.sql
		
		if [ "$type" != "1" ] 
		then
			# Stats on Existing Tables -- Collect all Existing Stats in PROD
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (1,'$prodDB','$table','$tempDB',line1);" >> $TEMPDIR/get_existing_stats.sql	
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (1,'$prodDB','$table','$prodDB',line1);" >> $TEMPDIR/create_prod_exisiting_stats.sql
			
			if [ "$PIChangeInd" -gt 0 ]
			then
				# Stats on Exisiting Tables -- Collect Stats on New Primary Index (Only for PI Change)
				echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (2,'$witsDB','$table','$tempDB',line1);" >> $TEMPDIR/get_new_stats.sql
				echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (2,'$witsDB','$table','$prodDB',line1);" >> $TEMPDIR/create_prod_new_stats.sql
			fi
			
		else
			# Stats on New Tables -- Collect Stats on Primary Index
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (2,'$witsDB','$table','$tempDB',line1);" >> $TEMPDIR/get_new_stats.sql
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (2,'$witsDB','$table','$prodDB',line1);" >> $TEMPDIR/create_prod_new_stats.sql
		fi

	done
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/get_existing_stats.sql $TEMPDIR/get_existing_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g' $TEMPDIR/get_existing_stats.out > $SQLDIR/"$relName"_migrate_stats_exist.sql
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/get_new_stats.sql $TEMPDIR/get_new_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g'  $TEMPDIR/get_new_stats.out > $SQLDIR/"$relName"_migrate_stats_new.sql
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/create_prod_exisiting_stats.sql $TEMPDIR/create_prod_exisiting_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g'  $TEMPDIR/create_prod_exisiting_stats.out > $SQLDIR/"$relName"_post_cutover_collect_stats_existing_tables.sql
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/create_prod_new_stats.sql $TEMPDIR/create_prod_new_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g'  $TEMPDIR/create_prod_new_stats.out > $SQLDIR/"$relName"_post_cutover_collect_stats_new_tables.sql
	
	echo "Stats Collection Scripts Created !!" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	
	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-9 Generation of Cutover Scripts.Rename Current Prod and Copy from TEMP to PROD.

	echo "Creating Cutover Scripts ... " >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	rm -f $SQLDIR/"$relName"_cutover_temp_to_prod_*.sql
	rm -f $SQLDIR/"$relName"_post_cutover_drop_temp_reporting_tables.sql
	rm -f $SQLDIR/"$relName"_cutover_prod_exisiting_rename.sql
	rm -f $SQLDIR/"$relName"_cutover_prod_exisiting_alter.sql
	rm -f $SQLDIR/"$relName"_cutover_prod_create_final.sql
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6,8,10 -d'|' | sort | uniq | while read -r line ; do      
		
		type=`echo $line | cut -f1 -d'|'`
		witsDB=`echo $line | cut -f2 -d'|'`
		tempDB=`echo $line | cut -f3 -d'|'`
		prodDB=`echo $line | cut -f4 -d'|'`
		table=`echo $line | cut -f5 -d'|'`
		calcInd=`echo $line | cut -f6 -d'|'`
		cutoverIndex=`echo $line | cut -f7 -d'|'`
		
		tableNameLen=`expr length "$table"`
		if [ $tableNameLen -gt 23 ]
		then
			newTableName=`echo $table | awk '{print substr($0,1,23)}'`
			newName=$prodDB.U_"$prefix"_"$newTableName"
			tempName=$prodDB.T_"$prefix"_"$newTableName"
		else
			newName=$prodDB.U_"$prefix"_"$table"
			tempName=$prodDB.T_"$prefix"_"$table"
		fi


		renamedTable=`cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND RENAME TABLE"  | grep -i -w "$prodDB" | grep -i -w "$table" | cut -f4 -d'|'`
		# Do not Create table if target table is being renamed from an existing prod table
		if [ "$renamedTable" != "$table" ]
		then
			# Cutover Script for Lead Database (or only reporting database)
			if [ "$type" != "1" ] 
			then
				echo "CREATE TABLE /*$ticketNo*/ $tempName, FALLBACK AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_temp_to_prod_"$cutoverIndex".sql

				echo "RENAME TABLE /*$ticketNo*/ $prodDB.\"$table\" TO $newName;" >> $SQLDIR/"$relName"_cutover_prod_exisiting_rename.sql
				echo "ALTER TABLE /*$ticketNo*/ $newName,NO FALLBACK;" >> $SQLDIR/"$relName"_cutover_prod_exisiting_alter.sql

				echo "RENAME TABLE /*$ticketNo*/ $tempName TO $prodDB.\"$table\";" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql		
				echo "DROP TABLE /*$ticketNo*/  $newName;" >> $SQLDIR/"$relName"_post_cutover_drop_temp_reporting_tables.sql
			else
				echo "CREATE TABLE /*$ticketNo*/ $prodDB.\"$table\", FALLBACK AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
			fi

			
			# Create Tables if Calculated Tables are being added
			if [ "$calcInd" == "Y" ]
			then
				if [ "$region" == "NC" ] || [ "$region" == "SC" ]
				then
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB1.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB2.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB3.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB4.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB5.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB6.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
					echo "CREATE TABLE /*$ticketNo*/  $prodCalcReportDB7.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_prod_create_final.sql
				fi
			fi
		fi
			
	done

	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND RENAME TABLE" | cut -f1,2,3,4 -d'|' | sort | uniq | while read -r line ; do      
		databaseName=`echo $line | cut -f2 -d'|'`
		table=`echo $line | cut -f3 -d'|'`
		newTable=`echo $line | cut -f4 -d'|'`
		
		if [ ! -z "$databaseName" ]
		then
			echo "RENAME TABLE /*$ticketNo*/  $databaseName.\"$table\" TO $databaseName.\"$newTable\" ;" >> $SQLDIR/"$relName"_cutover_prod_exisiting_rename.sql 
		fi
	done	
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND DROP TABLE" | cut -f1,2,3 -d'|' | sort | uniq | while read -r line ; do      
		databaseName=`echo $line | cut -f2 -d'|'`
		table=`echo $line | cut -f3 -d'|'`
		
		tableNameLen=`expr length "$table"`
		if [ tableNameLen -gt 23 ]
		then
			newTableName=`echo $table | awk '{print substr($0,1,23)}'`
			newName=$databaseName.U_"$prefix"_"$newTableName"
		else
			newName=$databaseName.U_"$prefix"_"$table"
		fi
		
		if [ ! -z "$databaseName" ]
		then
			echo "RENAME TABLE /*$ticketNo*/ $databaseName.\"$table\" TO $newName ;" >> $SQLDIR/"$relName"_cutover_prod_exisiting_rename.sql
			echo "ALTER TABLE /*$ticketNo*/ $newName,NO FALLBACK;" >> $SQLDIR/"$relName"_cutover_prod_exisiting_alter.sql 
			echo "DROP  TABLE /*$ticketNo*/ $newName;" >> $SQLDIR/"$relName"_post_cutover_drop_temp_reporting_tables.sql
		fi
	done
	
	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-10 Create the Staging Tables	

	echo "Creating Staging Table Scripts ... " >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_STG_LIST  -d $DIR/$stagingList -l $logFileName 


	echo " SELECT 'DROP TABLE /*$ticketNo*/' ||  TRIM(TAB.DatabaseName) || '.' ||  TRIM(TAB.TableName) || ';' AS DROP_CURRENT_TABLE " > $TEMPDIR/drop_staging_tables.sql
	echo " FROM DBC.TablesV TAB JOIN CLARITY_DBA_MAINT.UPG_STG_LIST LIST ON TRIM(TAB.TableName)=TRIM(LIST.STG_TABLE_NAME) " >> $TEMPDIR/drop_staging_tables.sql
	echo " WHERE LIST.STG_TABLE_NAME NOT LIKE ANY ('%tokenx%','%_ERROR2') AND TRIM(TAB.DatabaseName) LIKE ANY ('$prodStgDB'" >> $TEMPDIR/drop_staging_tables.sql
	if [ "$region" == "SC" ]
	then
		echo ",'HCCLPSC%_S'" >> $TEMPDIR/drop_staging_tables.sql
	fi
	if [ "$region" == "NC" ]
	then
		echo ",'HCCLPNC%_S'" >> $TEMPDIR/drop_staging_tables.sql
	fi
	if [ "$region" == "SC" ]
	then
		echo ",'$prodTpfStageDB','HCCLPSC%_TPF_S'" >> $TEMPDIR/drop_staging_tables.sql
	fi
	if [ "$region" == "NC" ]
	then
		echo ",'$prodTpfStageDB','HCCLPNC%_TPF_S'" >> $TEMPDIR/drop_staging_tables.sql
	fi
	echo ") GROUP BY 1;" >> $TEMPDIR/drop_staging_tables.sql

	
	rm -f $TEMPDIR/drop_staging_tables.out
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/drop_staging_tables.sql $TEMPDIR/drop_staging_tables.out | tee -a  $logFileName
	
	sed '1d' $TEMPDIR/drop_staging_tables.out > $SQLDIR/"$relName"_staging_tables_drop_current.sql
	
	
	rm -f $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
	rm -f $TEMPDIR/"$relName"_w2p_staging_tables_export_ddl.out
	rm -f $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
	rm -f $TEMPDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.out
	stagingTableCount="0"
	
	# Skip TokenX Tables while dropping and creating staging tables
	cat $DIR/$stagingList | grep -v -i "tokenx" | grep -v -i '_ERROR2' | while read -r line ; do

		action=`echo $line | cut -f1 -d'|'`
		tableName=`echo $line | cut -f2 -d'|'`
		
		if [ "$action" != "DROP" ]
		then
			echo "SHOW TABLE $devStgDB.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			stagingTableCount=`expr $stagingTableCount + 1`

							
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				echo "SHOW TABLE $devDeployStgDB1.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
				echo "SHOW TABLE $devDeployStgDB2.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
				echo "SHOW TABLE $devDeployStgDB3.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
				echo "SHOW TABLE $devDeployStgDB4.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
				echo "SHOW TABLE $devDeployStgDB5.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
				echo "SHOW TABLE $devDeployStgDB6.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql		
				echo "SHOW TABLE $devDeployStgDB7.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql		
				stagingTableCount=`expr $stagingTableCount + 6`

				tpfInd=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $tableName | wc -l`
				stgTabCount=`cat $SQLDIR/"$relName"_staging_tables_drop_current.sql | grep -i -w "$tableName" | wc -l`
				
				if [ $tpfInd -ne 0 ] || [ $stgTabCount -gt 7 ]
				then
					stagingTableCount=`expr $stagingTableCount + 7`
					echo "SHOW TABLE $devStgDB.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
					echo "SHOW TABLE $devDeployStgDB1.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
					echo "SHOW TABLE $devDeployStgDB2.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
					echo "SHOW TABLE $devDeployStgDB3.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
					echo "SHOW TABLE $devDeployStgDB4.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
					echo "SHOW TABLE $devDeployStgDB5.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql
					echo "SHOW TABLE $devDeployStgDB6.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql	
					echo "SHOW TABLE $devDeployStgDB7.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql	
				fi
			fi
		fi
			
	done
	
	# Copy From WITS to PROD
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql $TEMPDIR/"$relName"_w2p_staging_tables_export_ddl.out | tee -a  $logFileName
	sed -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$devStgDB'/'$prodStgDB'/g' \
	-e 's/'$devDeployStgDB1'/'$prodDeployStgDB1'/g' -e 's/'$devDeployStgDB2'/'$prodDeployStgDB2'/g' -e 's/'$devDeployStgDB3'/'$prodDeployStgDB3'/g' \
	-e 's/'$devDeployStgDB4'/'$prodDeployStgDB4'/g' -e 's/'$devDeployStgDB5'/'$prodDeployStgDB5'/g' -e 's/'$devDeployStgDB6'/'$prodDeployStgDB6'/g' \
	$TEMPDIR/"$relName"_w2p_staging_tables_export_ddl.out > $SQLDIR/"$relName"_staging_tables_create.sql
	
	# Create TPF PROD
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.sql $TEMPDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.out | tee -a  $logFileName
	sed -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$devStgDB'/'$prodTpfStageDB'/g' \
	-e 's/'$devDeployStgDB1'/'$prodTpfDeployStgDB1'/g' -e 's/'$devDeployStgDB2'/'$prodTpfDeployStgDB2'/g' -e 's/'$devDeployStgDB3'/'$prodTpfDeployStgDB3'/g' \
	-e 's/'$devDeployStgDB4'/'$prodTpfDeployStgDB4'/g' -e 's/'$devDeployStgDB5'/'$prodTpfDeployStgDB5'/g' -e 's/'$devDeployStgDB6'/'$prodTpfDeployStgDB6'/g' \
	$TEMPDIR/"$relName"_w2p_staging_tables_tpf_export_ddl.out > $SQLDIR/"$relName"_staging_tables_tpf_create.sql
	
	# Logic for Checking Secondary Index
	sed -e 's/MY_DB/'$prodStgDB'/g' -e 's/MY_TICKET/'$ticketNo'/g' -e 's/MY_REGION/'$region'/g' $SQLDIR/accdba_check_secondary_index.sql > $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql

	echo "-- Staging Table Total : $stagingTableCount " >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	
	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-11 Create TPF Stored Procedures	

	#--------------------------------------------------------------------------------
	# Input File must end with SRC; and have "  
	# Example -
	# FROM	"HCCLDSC9E_RESC_S"."IMMUNE" SRC;
	#--------------------------------------------------------------------------------
	rm -f $OUTDIR/"$relName"_tpf_store_proc_analysis.dat
	if [ -f $DIR/"$relName"_trace_code.txt ]
	then

		if [ ! -d $SQLDIR/archive/"$relName"/traceCode ]
		then
			mkdir $SQLDIR/archive/"$relName"/traceCode
		fi
		
		startPos="1"
		grep -i -n "SRC\;" $DIR/"$relName"_trace_code.txt | cut -f1 -d":" | while read -r line; do

			endPos=$line
			
			sed -n ''$startPos','$endPos' p' $DIR/"$relName"_trace_code.txt  > $TEMPDIR/trace_code.tmp
			
			inputStgDeployment=`tail -1 $TEMPDIR/trace_code.tmp | cut -f2 -d'"'`
			table=`tail -1 $TEMPDIR/trace_code.tmp | cut -f4 -d'"'`

			mv $TEMPDIR/trace_code.tmp  $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat
			
			startPos=`expr $endPos + 1`
			echo "$table|$inputStgDeployment" >> $OUTDIR/"$relName"_tpf_store_proc_analysis.dat
			
		done
	fi

	rm -f $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND IMPACTED TPF STORED PROCEDURE" | cut -f3 -d'|' | sort | uniq | while read -r tabLine ; do
	
		cat $OUTDIR/"$relName"_tpf_store_proc_analysis.dat | grep -i -w "$tabLine" > $TEMPDIR/tpf_store_proc_analysis.dat
		
		if [ -s $TEMPDIR/tpf_store_proc_analysis.dat ] && [ -s $SQLDIR/archive/"$relName"/traceCode/sp_"$tabLine".dat ]
		then
			table=`head -1 $TEMPDIR/tpf_store_proc_analysis.dat | cut -f1 -d'|'`
			inputStgDeployment=`head -1 $TEMPDIR/tpf_store_proc_analysis.dat | cut -f2 -d'|'`
			
			case "$inputStgDeployment" in
				"$devDeployStgDB1")
					traceDeployId=$deployId1
				;;
				"$devDeployStgDB2")
					traceDeployId=$deployId2
				;;
				"$devDeployStgDB3")
					traceDeployId=$deployId3
				;;
				"$devDeployStgDB4")
					traceDeployId=$deployId4
				;;
				"$devDeployStgDB5")
					traceDeployId=$deployId5
				;;
				"$devDeployStgDB6")
					traceDeployId=$deployId6
				;;
				"$devDeployStgDB7")
					traceDeployId=$deployId7
				;;
			esac
			
			# Add all checks - check for table and stagingDatabase
			check1=`cat $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat | grep -i $inputStgDeployment | wc -l`
			check2=`cat $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat | grep -i $table | wc -l`
			if [ "$check1" -gt 0 ] && [ "$check2" -gt 0 ]
			then
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB1 $prodTpfReportDB $prodTpfDeployStgDB1 $traceDeployId $deployId1
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB2 $prodTpfReportDB $prodTpfDeployStgDB2 $traceDeployId $deployId2
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB3 $prodTpfReportDB $prodTpfDeployStgDB3 $traceDeployId $deployId3
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB4 $prodTpfReportDB $prodTpfDeployStgDB4 $traceDeployId $deployId4
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB5 $prodTpfReportDB $prodTpfDeployStgDB5 $traceDeployId $deployId5
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB6 $prodTpfReportDB $prodTpfDeployStgDB6 $traceDeployId $deployId6
				create_tpf_storedProc $table $inputStgDeployment $prodDeployStgDB7 $prodTpfReportDB $prodTpfDeployStgDB7 $traceDeployId $deployId7
			else
				echo "Trace Code for $tabLine does not appear to be valid" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi
		else
			echo "Trace Code for $tabLine was not found" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
		fi
	
	done
	

#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-12 Final List of Impacted Materialized Views and View Load Scripts




#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-13 Check if all scripts are generated


	sleep 5
	procCount=`ps -ef | grep -i "epdba_upgrade_create_migration_scripts" | grep $USER | wc -l`
	while [ $procCount -gt 2 ]
	do
		sleep 30
		procCount=`ps -ef | grep -i "epdba_upgrade_create_migration_scripts" | grep $USER | wc -l`
	done
	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-14 Validate if scripts match object count in dbchange list

rm -f $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	
	newTabCount="0"
	newCalcTabCount="0"
	cat $DIR/$dbChgList | grep -w -i "Table Add" | cut -f2,6 -d'|' | sort | uniq | while read -r line; do
		table=`echo $line | cut -f1 -d'|'`
		calcInd=`echo $line | cut -f2 -d'|'`
		if [ "$calcInd" == "Y" ]
		then
			# Calculated - Across all 6 deployments
			newCalcTabCount=`expr $newCalcTabCount + 6`
		else
			# Only in Lead
			newTabCount=`expr $newTabCount + 1`
		fi
	done
	
	
	existTabCount="0"
	cat $DIR/$dbChgList | grep -w -v -i "Table Drop" | grep -w -v -i "Table Add"  | grep -w -v -i "Rename Table" | grep -w -v -i "View Only" | cut -f2,6 -d'|' | sort | uniq | while read -r line; do
		table=`echo $line | cut -f1 -d'|'`
		calcInd=`echo $line | cut -f2 -d'|'`
		if [ "$calcInd" == "Y" ]
		then
			# Calculated - Across all 6 deployments
			existTabCount=`expr $existTabCount + 6`
		else
			# Only in Lead
			existTabCount=`expr $existTabCount + 1`
		fi
	done

	reportingTableCount=`expr $newTabCount + $existTabCount + $newCalcTabCount`
	echo "-- Reporting Table Total : $reportingTableCount " >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql

	echo "DELETE FROM CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION WHERE RUN_ID=$runId AND TEST_CASE_NO IN ('1.1','1.2','2.1');" >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	
	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION " >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	echo "SELECT $runId, '1.1', 'New Tables Created in Temp',$newTabCount,COUNT(*), (COUNT(*) - $newTabCount) FROM DBC.TablesV WHERE TRIM(DatabaseName) IN ('HCCL"$region"_UPG_AK_TAB_ADD');  " >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	echo "INSERT INTO CLARITY_DBA_MAINT.CLARITY_UPG_VALIDATION "  >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	echo "SELECT $runId, '1.2', 'Existing Tables Created in Temp',$existTabCount,COUNT(*), (COUNT(*) - $existTabCount) FROM DBC.TablesV WHERE TRIM(DatabaseName) LIKE ANY ('HCCL"$region"_UPG_AK_%_CHANGE','HCCL"$region"_UPG_AK_TAB_CHANGE_%');" >> $SQLDIR/archive/$relName/"$relName"_validation_summary_scripts.sql
	

#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-15 WRAP UP and Send Approrpriate Notification

	rm -f $TEMPDIR/accdba_get_ddl.tmp

	echo "---------------------------------------------------------------" >> $logFileName
	echo "---------  Migration Scripts Succesfully Created !! -----------" >> $logFileName
	echo "------------ Ended at `date +%Y-%m-%d\ %H:%M:%S` ------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	
	