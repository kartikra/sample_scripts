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


	#runId=""
	#relName=""			#   Region + Cutover Date
	#ticketNo=""		#  Get From Remedy or Ask ETL team
    #regionProfile=""	#  Look at Db Change list and detrrmine which profile to use
	#region=""			#  Region of the upgrade 
       
	runId="4"
	relName="ga0626"
	ticketNo="CRQ000000211731"
    regionProfile="REGNGAM"
	region="GA"


	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
        fi

		
	#-------------------------------------------    Generate the Backup Scripts ----------------------------------------------

	#$SCRIPTDIR/epdba_perform_epupg_backup_analysis.sh > $LOGDIR/archive/"$relName"_backup_analysis.log 
	
	#$SCRIPTDIR/epdba_gen_epupg_migration_create_backup.sh > $LOGDIR/archive/"$relName"_generate_backup_scripts.log 
		
	
	#-------------------------------------------    Initial Creation of Backup ----------------------------------------------

	#$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 5 $SQLDIR/archive/$relName/  "$relName"_backup_create_tables.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_create_tables.log	
	
	#-------------------------------------------   Emable Block Level Compression ----------------------------------------------

	#$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/  "$relName"_backup_delete_data_from_backup_tables.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_enable_blocklevelcomp.log	
	#$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/  "$relName"_backup_enable_blocklevel_compression.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_enable_blocklevelcomp.log	

	#-------------------------------------------    Migration Run ----------------------------------------------
		
					# Load Backup Tables
	#rm -f $LOGDIR/archive/"$relName"_backup_migration_run.log
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_migrate_data_from_prod_tables.sql  $OUTDIR/drive_backup.out | tee -a $LOGDIR/archive/"$relName"_backup_migration_run.log
	#$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 5 $SQLDIR/archive/$relName/  "$relName"_backup_migrate_data_from_prod_tables.sql $OUTDIR/drive_backup.out $LOGDIR/archive/"$relName"_backup_migration_run.log	
	
					# Take Count of Backup Tables
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_source_table_count.dat $TDPROD $ticketNo $runId 101 >> $LOGDIR/archive/"$relName"_backup_migration_run.log
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_backup_table_count.dat $TDPROD $ticketNo $runId 201 >> $LOGDIR/archive/"$relName"_backup_migration_run.log

	
	
					# Create UPGR_Views
	#logFileName="$LOGDIR/archive/"$relName"_upgr_view_creation.log"
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_simple.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_additional.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_complex.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup__view_materialize.sql $OUTDIR/drive_migration.out | tee -a $logFileName

	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup_user_view_simple.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup_user_view_additional.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	#$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_backup_user_view_complex.sql $OUTDIR/drive_migration.out | tee -a $logFileName
	
					# Take Count of UPGR_VIEWS
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_original_view_count.dat $TDPROD $ticketNo $runId 102 > $LOGDIR/archive/"$relName"_backup_create_upgr_views.log
	#$SCRIPTDIR/epdba_load_audit.sh "$relName"_upgr_view_count.dat $TDPROD $ticketNo $runId 202 > $LOGDIR/archive/"$relName"_backup_create_upgr_views.log

	