#!/usr/bin/ksh

	relName=$1
	ticketNo=$2
	regionProfile=$3
	dbChgList=$4
	stagingList=$5
	mockInd=$6
	
	
	
# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi

	
# STEP-2 Run Region Profile File

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

	. $SQLDIR/archive/"$relName"/runid.profile
	
		
	scriptName=`basename $0`
	rm -f $TEMPDIR/"$scriptName"_email_attach.dat
	rm -f $TEMPDIR/"$scriptName"_email_additional_info.dat

	
# Define Functions
#-------------------------------------------------------------------------------------------------------------------------------------

send_email () {

emailStatus=$1
stepDescription=$2

	ename=`echo $relName | tr '[a-z]' '[A-Z]'`
	emailSubjectLine="$ename  UPGRADE STATUS - Completed $stepDescription"
	rm -f $TEMPDIR/"$scriptName"_email_body.dat
	
	if [ -s $TEMPDIR/"$scriptName"_email_additional_info.dat ]
	then
		cat $TEMPDIR/"$scriptName"_email_additional_info.dat >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "" >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	
	if [ $emailStatus == "FAILURE" ]
	then
		echo "$stepDescription for $relName completed with errors." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Scripts that failed during execution are attached to this email. Analyze them before proceeding to next step."  >>  $TEMPDIR/"$scriptName"_email_body.dat
	fi
	if [ $emailStatus == "SUCCESS" ]
	then
		echo "$stepDescription for $relName completed without any errors. Please proceed to next step" >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	if [ $emailStatus == "WARNING" ]
	then
		echo "$stepDescription for $relName completed without any execution errors." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "However 1 or more validation issues have been found. Look at log files and audit tables for details." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Please do not proceed to next step without resolving validation issues." >> $TEMPDIR/"$scriptName"_email_body.dat
	fi
	
	
	if [ $emailStatus != "FAILURE" ]
	then
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
	else
		# FAILURE - Make sure only DBA group is notified
		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
	fi

}	
	

create_mock_cutover_scripts ()
{
	cp $1 $2
	
	perl -pi -e 's/''WITH\ DATA\ AND\ STATS''/''WITH\ NO\ DATA''/gi'  $2
	# perl -pi -e 's/'HCCLP'/''HCCL'$region'_UPG_DRYRUN_HCCLP''/gi'  $2

	grep -i -w -n  "$prodStgDB" $2 > $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodReportDB" $2 >> $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodMatReportDB" $2 >> $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodKPBIReportDB" $2 >> $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodView" $2 >> $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodMatView" $2 >> $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodKPBIView" $2 >> $TEMPDIR/get_cols.tmp
	grep -i -w -n  "$prodUserView" $2 >> $TEMPDIR/get_cols.tmp

	# Remove "
	cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
			sed  ''$lineNumber's/\"//g'  $2 > $TEMPDIR/ref_view.tmp
			mv $TEMPDIR/ref_view.tmp $2
	done
	
	# Remove Spaces
	perl -pi -e 's/'$prodStgDB'\ *\./'$prodStgDB'\./gi'  $2
	perl -pi -e 's/'$prodReportDB'\ *\./'$prodReportDB'\./gi'  $2
	perl -pi -e 's/'$prodMatReportDB'\ *\./'$prodMatReportDB'\./gi'  $2
	perl -pi -e 's/'$prodKPBIReportDB'\ *\./'$prodKPBIReportDB'\./gi'  $2
	perl -pi -e 's/'$prodView'\ *\./'$prodView'\./gi'  $2
	perl -pi -e 's/'$prodMatView'\ *\./'$prodMatView'\./gi'  $2
	perl -pi -e 's/'$prodKPBIView'\ *\./'$prodKPBIView'\./gi'  $2
	perl -pi -e 's/'$prodUserView'\ *\./'$prodUserView'\./gi'  $2
	
	
	perl -pi -e 's/'$prodStgDB'\./'$dryrunStgDB'\./gi'  $2
	perl -pi -e 's/'$prodReportDB'\./'$dryrunReportDB'\./gi'  $2
	perl -pi -e 's/'$prodMatReportDB'\./'$dryrunMatReportDB'\./gi'  $2
	perl -pi -e 's/'$prodKPBIReportDB'\./'$dryrunKPBIReportDB'\./gi'  $2
	perl -pi -e 's/'$prodView'\./'$dryrunView'\./gi'  $2
	perl -pi -e 's/'$prodMatView'\./'$dryrunMatView'\./gi'  $2
	perl -pi -e 's/'$prodKPBIView'\./'$dryrunKPBIView'\./gi'  $2
	perl -pi -e 's/'$prodUserView'\./'$dryrunUserView'\./gi'  $2

 
 	# perl -pi -e 's/\"'$prodStgDB'\"\./'$dryrunStgDB'\./gi'  $2
	# perl -pi -e 's/\"'$prodReportDB'\"\./'$dryrunReportDB'\./gi'  $2
	# perl -pi -e 's/\"'$prodView'\"\./'$dryrunView'\./gi'  $2
	# perl -pi -e 's/\"'$prodMatReportDB'\"\./'$dryrunMatReportDB'\./gi'  $2
	# perl -pi -e 's/\"'$prodMatView'\"\./'$dryrunMatView'\./gi'  $2
	
	# perl -pi -e 's/\"'$prodUserView'\"\./'$dryrunUserView'\./gi'  $2
	# perl -pi -e 's/\"'$prodKPBIReportDB'\"\./'$dryrunKPBIReportDB'\./gi'  $2
	# perl -pi -e 's/\"'$prodKPBIView'\"\./'$dryrunKPBIView'\./gi'  $2
 
 
} 


run_multiple_sqlFile()
{
	TDHOST=$1
	sqlFileName=$2
	outFileName=$3
	logFileName=$4
	fileCount=$5
	

	$SCRIPTDIR/epdba_split_file.sh "REPLACE VIEW" $sqlFileName $fileCount

	i="1"
	while [ $i -le $fileCount ]
	do
		$SCRIPTDIR/epdba_runSQLFile.sh $TDHOST "$sqlFileName"_"$i"  "$outFileName"_"$i" > "$logFileName"_"$i".log &
		sleep 2
		i=`expr $i + 1`
	done
	
	
	row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i $sqlFileName | wc -l`
	while [ $row_cnt -gt 1 ]
	do
		sleep 10
		row_cnt=`ps -ef | grep $USER | grep -i epdba_runSQLFile | grep -i $sqlFileName | wc -l`
	done
	
	
	i="1"
	while [ $i -le $fileCount ]
	do
		if [ -f "$logFileName"_"$i" ]
		then
			cat "$logFileName"_"$i".log >> $logFileName
			rm -f "$logFileName"_"$i".log
		fi
		
		if [ -f "$outFileName"_"$i" ]
		then
			cat "$outFileName"_"$i" >> $outFileName
			rm -f "$outFileName"_"$i"
		fi
		
		if [ -f "$sqlFileName"_"$i" ]
		then
			rm -f "$sqlFileName"_"$i"
		fi
		
		i=`expr $i + 1`
	done
	
	
}


mock_cutover_refresh_views()
{

	. $HOMEDIR/region/DRYRUN_"$region".profile
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "Profile file DRYRUN_"$region".profile cannot be found, Exiting"
			exit 902
		fi

		
	#-----------------------------------------  View Refresh Steps  ------------------------------------------#
	viewlogFileName="$LOGDIR/archive/"$relName"_mock_cutover_refresh_views.log"

			#  VIEWS
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_copy_from_wits.sql ]
	then
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_copy_from_wits.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_copy_from_wits.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_copy_from_wits.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_create_new.sql ]
	then
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_create_new.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_create_new.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_create_new.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_with_dr.sql ]
	then
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_with_dr.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_refresh_with_dr.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_refresh_with_dr.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_without_dr.sql ]
	then
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_without_dr.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_refresh_without_dr.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view_refresh_without_dr.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	
	
			# USER VIEWS
	
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_create_new.sql ]
	then	
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_create_new.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_create_new.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_create_new.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_without_dr.sql ]
	then	
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_without_dr.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_refresh_without_dr.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_refresh_without_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql ]
	then	
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql ]
	then	
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_with_dr.sql ]
	then	
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_with_dr.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_refresh_with_dr.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_refresh_with_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_custom_views.sql ]
	then	
		create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_custom_views.sql $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_refresh_custom_views.sql
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview_refresh_custom_views.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 10
	fi
	

	
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view*.sql | grep -i -w $prodView > $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view*.sql | grep -i -w $prodMatView >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view*.sql | grep -i -w $prodKPBIView >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view*.sql | grep -i -w $prodReportDB >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view*.sql | grep -i -w $prodMatReportDB >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_view*.sql | grep -i -w $prodKPBIReportDB >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview*.sql | grep -i -w $prodUserView >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview*.sql | grep -i -w $prodView >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview*.sql | grep -i -w $prodMatView >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	cat $SQLDIR/archive/$relName/dryrun/"$relName"_cutover_prod_userview*.sql | grep -i -w $prodKPBIView >> $SQLDIR/archive/$relName/dryrun/view_replacement_exceptions.dat
	
	
}

	
	
cutover_refresh_views()
{

	#-----------------------------------------  View Refresh Steps  ------------------------------------------#
	viewlogFileName="$LOGDIR/archive/"$relName"_cutover_refresh_views.log"

			#  VIEWS
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_copy_from_wits.sql ]
	then
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_copy_from_wits.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_create_new.sql ]
	then
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_create_new.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_with_dr.sql ]
	then
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_with_dr.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_without_dr.sql ]
	then
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_view_refresh_without_dr.sql  $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	
	
			# USER VIEWS
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_custom_views.sql ]
	then	
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_custom_views.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 10
	fi		
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_create_new.sql ]
	then	
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_create_new.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_without_dr.sql ]
	then	
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_without_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql ]
	then	
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql ]
	then	
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 2
	fi
	if [ -f $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_with_dr_final.sql ]
	then	
		run_multiple_sqlFile "$TDPROD" $SQLDIR/archive/$relName/"$relName"_cutover_prod_userview_refresh_with_dr_final.sql $OUTDIR/drive_viewrefresh.out $viewlogFileName 6
	fi
	
	

}


#---------------------------------------------------------------------------------------------------------------------------
# MAIN PROGRAM
#---------------------------------------------------------------------------------------------------------------------------

	rm -f $TEMPDIR/"$scriptName"_email_attach.dat

	if [ "$mockInd" == 'M' ]
	then

	#---------------------------------- MOCK DRY RUN -----------------------------------------------------#
	
		. $HOMEDIR/region/DRYRUN_"$region".profile
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "Profile file DRYRUN_"$region".profile cannot be found, Exiting"
			exit 902
		fi
	
		
		reflogFileName="$LOGDIR/log1.log"
		$SCRIPTDIR/epdba_runFastExport.sh -h $TDDEV -i $devReportDB.CLARITY_TBL -d $IMPEXP/out_"$devReportDB".CLARITY_TBL.dat -l $reflogFileName
		$SCRIPTDIR/epdba_runFastExport.sh -h $TDDEV -i $devReportDB.CLARITY_COL -d $IMPEXP/out_"$devReportDB".CLARITY_COL.dat -l $reflogFileName

		$SCRIPTDIR/epdba_runFastLoad.sh -h "$TDPROD" -d "$IMPEXP/out_"$devReportDB".CLARITY_TBL.dat"  -o "$dryrunReportDB.CLARITY_TBL"  -l "$reflogFileName"
		$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o $dryrunReportDB.CLARITY_COL -d $IMPEXP/out_"$devReportDB".CLARITY_COL.dat -l $reflogFileName
	
	
		# Refresh Views
		viewlogFileName="$LOGDIR/archive/"$relName"_mock_cutover_refresh_views.log"
		rm -f $viewlogFileName
		
		mock_cutover_refresh_views

		# Check For Difference in Columns between Reporting,  View and User View
		cat $SQLDIR/accdba_validate_mock_cutover.sql | sed -e 's/MY_RUN_ID/'$runId'/g' \
		-e 's/MY_DRYRUN_REPORT_DB/'$dryrunReportDB'/g' -e 's/MY_DRYRUN__DB/'$dryrunView'/g' -e 's/MY_DRYRUN_USER_DB/'$dryrunUserView'/g' \
		-e 's/MY_REPORT_DB/'$prodReportDB'/g' -e 's/MY__DB/'$prodView'/g' -e 's/MY_USER_DB/'$prodUserView'/g' \
		> $SQLDIR/archive/$relName/dryrun/"$relName"_validate_mock_cutover.sql
		
		rm -f $OUTDIR/"$relName"_validation_mock_cutover_objects.out
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/dryrun/"$relName"_validate_mock_cutover.sql $OUTDIR/"$relName"_validate_mock_cutover.out | tee -a $viewlogFileName

		# Check if All Views Are Accessible
		if [ -f $SQLDIR/archive/$relName/"$relName"_validation_userview_access.sql ]
		then
			create_mock_cutover_scripts $SQLDIR/archive/$relName/"$relName"_validation_userview_access.sql $SQLDIR/archive/$relName/dryrun/"$relName"_validation_userview_access.sql
			$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/dryrun/ "$relName"_validation_userview_access.sql $OUTDIR/validate_userview_access.out $viewlogFileName	
		fi
		
		
		# Email Generation
		rm -f $LOGDIR/archive/"$relName"_errors_mock_cutover_refresh_views.log
		$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_mock_cutover_refresh_views.log $LOGDIR/archive/"$relName"_errors_mock_cutover_refresh_views.log
		
		if [ -s "$OUTDIR/"$relName"_validate_mock_cutover.out" ]
		then
			echo "Some objects are missing across Reporting,  View and User View Layers. Please find the Object Comparison results attached to this email." > $TEMPDIR/"$scriptName"_email_additional_info.dat	
			echo "$OUTDIR|"$relName"_validate_mock_cutover.out|"$relName"_validate_mock_cutover.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			echo "User View Validation is complete. All objects are present across Reporting,  View and User View Layers." > $TEMPDIR/"$scriptName"_email_additional_info.dat	
		fi
		
		
		stepDescription="Mock Cutover  and User View Refresh"
		if [ -s $LOGDIR/archive/"$relName"_errors_mock_cutover_refresh_views.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			echo "$LOGDIR/archive|"$relName"_errors_mock_cutover_refresh_views.log|"$relName"_errors_mock_cutover_refresh_views.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			if [ "$emailStatus" != "WARNING" ]
			then
				# No Errors Found
				emailStatus="SUCCESS"
			fi
		fi
		send_email $emailStatus "$stepDescription"
	
	
		
		
	else
	#---------------------------------- PRODUCTION CUTOVER RUN -----------------------------------------------------#

		# Refresh Views
		viewlogFileName="$LOGDIR/archive/"$relName"_cutover_refresh_views.log"
		rm -f $viewlogFileName
		
		cutover_refresh_views
		
		# Check For Difference in Columns between Reporting,  View and User View
		cat $SQLDIR/accdba_validate_cutover_objects.sql | sed -e 's/MY_RUN_ID/'$runId'/g' -e's/MY_REGION/'$region'/g' > $SQLDIR/archive/$relName/"$relName"_validation_cutover_objects.sql
		
		rm -f $OUTDIR/"$relName"_validation_cutover_objects.out
		$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/archive/$relName/"$relName"_validation_cutover_objects.sql $OUTDIR/"$relName"_validation_cutover_objects.out | tee -a $viewlogFileName

		
		# Check if All Views Are Accessible
		if [ -f $SQLDIR/archive/$relName/"$relName"_validation_userview_access.sql ]
		then
			$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDPROD 10 $SQLDIR/archive/$relName/ "$relName"_validation_userview_access.sql $OUTDIR/validate_userview_access.out $viewlogFileName	
		fi
		
					# Email Generation
		rm -f $LOGDIR/archive/"$relName"_errors_cutover_refresh_views.log
		$SCRIPTDIR/epdba_get_logerrors.sh $LOGDIR/archive/ "$relName"_cutover_refresh_views.log $LOGDIR/archive/"$relName"_errors_cutover_refresh_views.log
		
		if [ -s "$OUTDIR/"$relName"_validation_cutover_objects.out" ]
		then
			echo "Some objects are missing across Reporting,  View and User View Layers. Please find the Object Comparison results attached to this email." > $TEMPDIR/"$scriptName"_email_additional_info.dat	
			echo "$OUTDIR|"$relName"_validation_cutover_objects.out|"$relName"_validation_cutover_objects.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			echo "User View Validation is complete. All objects are present across Reporting,  View and User View Layers." > $TEMPDIR/"$scriptName"_email_additional_info.dat	
		fi
		
		
		stepDescription="Cutover  and User View Refresh"
		if [ -s $LOGDIR/archive/"$relName"_errors_cutover_refresh_views.log ]
		then
			# Errors Found in Execution
			emailStatus="FAILURE"
			echo "$LOGDIR/archive|"$relName"_errors_cutover_refresh_views.log|"$relName"_errors_cutover_refresh_views.txt" >> $TEMPDIR/"$scriptName"_email_attach.dat
		else
			if [ "$emailStatus" != "WARNING" ]
			then
				# No Errors Found
				emailStatus="SUCCESS"
			fi
		fi
		send_email $emailStatus "$stepDescription"
		
	fi
		
		
		
		