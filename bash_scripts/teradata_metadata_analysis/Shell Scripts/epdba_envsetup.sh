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


	srcProfile="PROD_HI"
	tgtProfile="REGNHIM"
	ticketNo="WO0000004469346"
	
	
	
# STEP-2 Run Source and Target Profile Files

	echo "Running Source Regional Profile File - $srcProfile.profile " >> $logFileName
	USR_PROF=$HOMEDIR/region/$srcProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $srcProfile.profile cannot be found, Exiting" >> $logFileName
                exit 902
		else
			if [ ! -z "$TDDEV" ]
			then
				TDSOURCE="$TDDEV"
				TDDEV=""
				srcStgDB="$devStgDB"
				srcDeployStageDB1="$devDeployStgDB1"
				srcDeployStageDB2="$devDeployStgDB2"
				srcDeployStageDB3="$devDeployStgDB3"
				srcDeployStageDB4="$devDeployStgDB4"
				srcDeployStageDB5="$devDeployStgDB5"
				srcDeployStageDB6="$devDeployStgDB6"
				srcDeployStageDB7="$devDeployStgDB7"
	
				srcReportDB="$devReportDB"
				srcView="$devView"
				srcUserView="$devUserView"
				srcCalcReportDB1="$devCalcReportDB1"
				srcCalcReportDB2="$devCalcReportDB2"
				srcCalcReportDB3="$devCalcReportDB3"
				srcCalcReportDB4="$devCalcReportDB4"
				srcCalcReportDB5="$devCalcReportDB5"
				srcCalcReportDB6="$devCalcReportDB6"
				srcCalcReportDB7="$devCalcReportDB7"
				srcMatReportDB="$devMatReportDB"
				srcMatView="$devMatView"
				
			fi
			if [ ! -z "$TDPROD" ]
			then
				TDSOURCE="$TDPROD"
				srcStgDB="$prodStgDB"
				srcDeployStageDB1="$prodDeployStgDB1"
				srcDeployStageDB2="$prodDeployStgDB2"
				srcDeployStageDB3="$prodDeployStgDB3"
				srcDeployStageDB4="$prodDeployStgDB4"
				srcDeployStageDB5="$prodDeployStgDB5"
				srcDeployStageDB6="$prodDeployStgDB6"
				srcDeployStageDB7="$prodDeployStgDB7"
				
				srcReportDB="$prodReportDB"
				srcView="$prodView"
				srcUserView="$prodUserView"
				srcCalcReportDB1="$prodCalcReportDB1"
				srcCalcReportDB2="$prodCalcReportDB2"
				srcCalcReportDB3="$prodCalcReportDB3"
				srcCalcReportDB4="$prodCalcReportDB4"
				srcCalcReportDB5="$prodCalcReportDB5"
				srcCalcReportDB6="$prodCalcReportDB6"
				srcCalcReportDB7="$prodCalcReportDB7"
				srcMatReportDB="$prodMatReportDB"
				srcMatView="$prodMatView"
			fi
        fi

	echo "Running Target Regional Profile File - $tgtProfile.profile " >> $logFileName
	USR_PROF=$HOMEDIR/region/$tgtProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $tgtProfile.profile cannot be found, Exiting" >> $logFileName
                exit 902
        else
			if [ ! -z "$TDDEV" ]
			then
				runName="$devrunName"
				TDTARGET="$TDDEV"
				tgtStgDB="$devStgDB"
				tgtDeployStageDB1="$devDeployStgDB1"
				tgtDeployStageDB2="$devDeployStgDB2"
				tgtDeployStageDB3="$devDeployStgDB3"
				tgtDeployStageDB4="$devDeployStgDB4"
				tgtDeployStageDB5="$devDeployStgDB5"
				tgtDeployStageDB6="$devDeployStgDB6"
				tgtDeployStageDB7="$devDeployStgDB7"
				
				tgtReportDB="$devReportDB"
				tgtView="$devView"
				tgtUserView="$devUserView"
				tgtCalcReportDB1="$devCalcReportDB1"
				tgtCalcReportDB2="$devCalcReportDB2"
				tgtCalcReportDB3="$devCalcReportDB3"
				tgtCalcReportDB4="$devCalcReportDB4"
				tgtCalcReportDB5="$devCalcReportDB5"
				tgtCalcReportDB6="$devCalcReportDB6"
				tgtCalcReportDB7="$devCalcReportDB7"
				tgtMatReportDB="$devMatReportDB"
				tgtMatView="$devMatView"
			else
				echo "Target is Not in WITS - Check the configuration. Aborting Script"
				exit 911
			fi
        fi
	

# STEP-3  Update CLARITY_UPDATE_WITS_PROFILE
	
	echo "CALL CLARITY_DBA_MAINT.CLARITY_UPDATE_WITS_PROFILE ('$tgtProfile','$srcProfile','$ticketNo','',ResultStr)" > $TEMPDIR/clarity_update_wits_profile.sql
	$SCRIPTDIR/epdba_runSQLFile2.sh "tdd1.kp.org" $TEMPDIR/clarity_update_wits_profile.sql $TEMPDIR/clarity_update_wits_profile.out | tee -a  $logFileName
	
	currBaselineRegion=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f2 -d'|'`
	ticketNo=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f3 -d'|'`
	currmanifestFile=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f4 -d'|'`
	
	if [ -z "$ticketNo" ]
	then
		echo "Workorder Not Found"
		exit 901
	fi
	
	
	migProfile="setup_"$tgtProfile""
	if [ ! -z "$tgtReportDB" ] && [ ! -z "$tgtView" ] && [ ! -z "$tgtUserView" ] && [ ! -z "$srcReportDB" ] && [ ! -z "$srcView" ] && [ ! -z "$srcUserView" ]
	then
		rm -f $OUTDIR/"$migProfile".list
		echo "$srcStgDB|$tgtStgDB|T"  					>> $OUTDIR/"$migProfile".list	
		echo "$srcReportDB|$tgtReportDB|T" 				>> $OUTDIR/"$migProfile".list
		if [ "$region" == "NC" ] || [ "$region" == "SC" ]
		then
			echo "$srcCalcReportDB1|$tgtCalcReportDB1|T" 	>> $OUTDIR/"$migProfile".list
			echo "$srcCalcReportDB2|$tgtCalcReportDB2|T" 	>> $OUTDIR/"$migProfile".list
			echo "$srcCalcReportDB3|$tgtCalcReportDB3|T" 	>> $OUTDIR/"$migProfile".list
			echo "$srcCalcReportDB4|$tgtCalcReportDB4|T" 	>> $OUTDIR/"$migProfile".list
			echo "$srcCalcReportDB5|$tgtCalcReportDB5|T" 	>> $OUTDIR/"$migProfile".list
			echo "$srcCalcReportDB6|$tgtCalcReportDB6|T" 	>> $OUTDIR/"$migProfile".list
			echo "$srcCalcReportDB7|$tgtCalcReportDB7|T" 	>> $OUTDIR/"$migProfile".list
		fi

		if [ "$region" == "CO" ] && [ ! -z "$prodOtherDB1" ]
		then
			echo "$prodOtherDB1|$devOtherDB1|T"  >> $OUTDIR/"$migProfile".list
		fi
		
		echo "$srcMatView|$tgtMatView|V" 		>> $OUTDIR/"$migProfile".list
		echo "$srcView|$tgtView|V"  			>> $OUTDIR/"$migProfile".list
		echo "$srcUserView|$tgtUserView|V"  			>> $OUTDIR/"$migProfile".list
		chmod 777 $OUTDIR/"$migProfile".list
	else
		echo "Not All Databases were picked up correctly. Aborting Script."
		exit 911
	fi
	
		
	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS : Starting Environment Set Up for $runName " >> $LOGDIR/archive/"$runName"_envsetup.log
	
	
	
	# Generate  and User Views that needs to be run in the Background
	echo "#!/usr/bin/ksh" > $TEMPDIR/"$runName"_export_views.sh
	echo "$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcView -t $tgtView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile".list -x 5" >> $TEMPDIR/"$runName"_export_views.sh
	echo "$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcUserView -t $tgtUserView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile".list -u Y -x 5" >> $TEMPDIR/"$runName"_export_views.sh
	chmod 775 $TEMPDIR/"$runName"_export_views.sh
	$TEMPDIR/"$runName"_export_views.sh > $LOGDIR/archive/"$runName"_export_views.log &
	sleep 10
	
	
# ---------------------------------------------------------------------------------------------------------------------------------------------------------
# STEP-4  Copying the Reporting Databases (ETC 6-8 hours)
	
	
	$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcReportDB -t $tgtReportDB -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log

	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS : Lead Reporting Tables copied" >> $LOGDIR/archive/"$runName"_envsetup.log
	
	
	if [ "$region" == "NC" ] || [ "$region" == "SC" ]
	then
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB1 -t $tgtCalcReportDB1 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB2 -t $tgtCalcReportDB2 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB3 -t $tgtCalcReportDB3 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB4 -t $tgtCalcReportDB4 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB5 -t $tgtCalcReportDB5 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB6 -t $tgtCalcReportDB6 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcCalcReportDB1 -t $tgtCalcReportDB7 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log

		updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		echo "$updateTS : Calculated Reporting Tables (A-G) copied" >> $LOGDIR/archive/"$runName"_envsetup.log
	fi
	
	
	
	if [ ! -z "$tgtMatReportDB" ] && [ ! -z "$srcMatReportDB" ]
	then
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcMatReportDB -t $tgtMatReportDB -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		
		
		if [ ! -z "$tgtMatView" ] && [ ! -z "$srcMatView" ]
		then
			echo "$srcMatReportDB|$tgtMatReportDB|T" 		> $OUTDIR/"$migProfile"_matview.list
			echo "$srcMatView|$tgtMatView|V" 		>> $OUTDIR/"$migProfile"_matview.list
			$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcMatView -t $tgtMatView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile"_matview.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		fi
	fi
	
	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS : Materialized Tables and Views copied" >> $LOGDIR/archive/"$runName"_envsetup.log
	
	
	
# STEP-5  Creation of  and USER Views	(ETC 4-6 hours)
	
	
	# Ensure that creation of VIEWS is complete before proceeding
	row_cnt=`ps -ef | grep $USER | grep -i epdba_copy_structure | grep -i $runName | egrep -i "$tgtView|$tgtUserView" | wc -l`
    while [ $row_cnt -gt 1 ]
	do
		sleep 10
		row_cnt=`ps -ef | grep $USER | grep -i epdba_copy_structure | grep -i $runName | egrep -i "$tgtView|$tgtUserView" | wc -l`
	done
	
	# Create  Views
	$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcView -t $tgtView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile".list -x 3 > $LOGDIR/archive/"$runName"_validate_envsetup.log
	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS :  Views copied" >> $LOGDIR/archive/"$runName"_envsetup.log
	
	# Create USER Views
	$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcUserView -t $tgtUserView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile".list -u Y -x 3 > $LOGDIR/archive/"$runName"_validate_envsetup.log
	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS : User Views copied" >> $LOGDIR/archive/"$runName"_envsetup.log
	
	
	
# STEP-6  Copying the Staging Databases (ETC 4-6 hours for Lead. 28-42 for deployments)

	$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcStgDB -t $tgtStgDB -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS : Lead Staging Tables copied" >> $LOGDIR/archive/"$runName"_envsetup.log

	if [ "$region" == "NC" ] || [ "$region" == "SC" ]
	then
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB1 -t $tgtDeployStageDB1 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB2 -t $tgtDeployStageDB2 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB3 -t $tgtDeployStageDB3 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB4 -t $tgtDeployStageDB4 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB5 -t $tgtDeployStageDB5 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB6 -t $tgtDeployStageDB6 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB1 -t $tgtDeployStageDB7 -w $ticketNo -o T -c 6  > $LOGDIR/archive/"$runName"_validate_envsetup.log

		updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		echo "$updateTS : Deployment Staging Tables (A-G) copied" >> $LOGDIR/archive/"$runName"_envsetup.log
		
	fi
	
	
# STEP-7 Copy KPBI Database and backup data. Refresh all KPBI Views if they exist in WITS
	
	# if [ ! -z "$devKPBIReportDB" ] && [ ! -z "$prodKPBIReportDB" ]
	# then	
		# $SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $prodKPBIReportDB -t $devKPBIReportDB  -w $ticketNo -o T -c 6 -d Y > $LOGDIR/archive/"$runName"_validate_envsetup.log
		
		# if [ ! -z "$devKPBIView" ] && [ ! -z "$prodKPBIView" ]
		# then
			# echo "$srcMatReportDB|$tgtMatReportDB|T" 		> $OUTDIR/"$migProfile"_kpbi.list
			# echo "$prodKPBIView|$devKPBIView|V" 		>> $OUTDIR/"$migProfile"_kpbi.list
			# $SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $prodKPBIView -t $devKPBIView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile"_kpbi.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		# fi
		
		# if [ ! -z "$prodKPBIUserView" ] && [ ! -z "$devKPBIUserView" ]
		# then
			# echo "$srcMatReportDB|$tgtMatReportDB|T" 		> $OUTDIR/"$migProfile"_kpbi.list
			# echo "$prodKPBIView|$devKPBIView|V" 		>> $OUTDIR/"$migProfile"_kpbi.list
			# echo "$prodKPBIUserView|$devKPBIUserView|V" 		>> $OUTDIR/"$migProfile"_kpbi.list
			# $SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName  -s $prodKPBIUserView -t $devKPBIUserView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile"_kpbi.list -u Y  > $LOGDIR/archive/"$runName"_validate_envsetup.log
		# fi
		
		
		# updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		# echo "$updateTS : KPBI Tables and Views copied" >> $LOGDIR/archive/"$runName"_envsetup.log
	# fi
	
	
	
# STEP-8 For SCAL Only, Refresh the TPF Databases and Stored Procedures

	if [ "$region" == "SC" ] || [ "$region" == "NC" ]
	then
	
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $prodTpfStageDB -t $devTpfStageDB -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
	
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB1 -t $devTpfDeployStgDB1 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB2 -t $devTpfDeployStgDB2 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB3 -t $devTpfDeployStgDB3 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB4 -t $devTpfDeployStgDB4 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB5 -t $devTpfDeployStgDB5 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB6 -t $devTpfDeployStgDB6 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $srcDeployStageDB1 -t $devTpfDeployStgDB7 -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log

		updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		echo "$updateTS : TPF Staging Tables Lead + (A-G) copied" >> $LOGDIR/archive/"$runName"_envsetup.log
		
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $prodTpfReportDB -t $devTpfReportDB -w $ticketNo -o T -c 6 > $LOGDIR/archive/"$runName"_validate_envsetup.log

		updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		echo "$updateTS : TPF Reporting Tables copied" >> $LOGDIR/archive/"$runName"_envsetup.log
		
		
		echo "$prodTpfReportDB|$devTpfReportDB|T" 		> $OUTDIR/"$migProfile"_tpf.list
		echo "$prodTpfView|$devTpfView|V" 		>> $OUTDIR/"$migProfile"_tpf.list
		echo "$prodTpfUserView|$devTpfUserView|V" 		>> $OUTDIR/"$migProfile"_tpf.list
		
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $prodTpfView -t $devTpfView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile"_tpf.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s $prodTpfUserView -t $devTpfUserView -w $ticketNo -o V -c 9 -m $OUTDIR/"$migProfile"_tpf.list -u Y > $LOGDIR/archive/"$runName"_validate_envsetup.log

		updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		echo "$updateTS : TPF  and User Views copied" >> $LOGDIR/archive/"$runName"_envsetup.log
		
		
		echo "$prodTpfReportDB|$devTpfReportDB|T"  > $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB1|$devTpfDeployStgDB1|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB2|$devTpfDeployStgDB2|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB3|$devTpfDeployStgDB3|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB4|$devTpfDeployStgDB4|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB5|$devTpfDeployStgDB5|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB6|$devTpfDeployStgDB6|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodTpfDeployStgDB7|$devTpfDeployStgDB7|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		
		echo "$prodDeployStgDB1|$devDeployStgDB1|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodDeployStgDB2|$devDeployStgDB2|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodDeployStgDB3|$devDeployStgDB3|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodDeployStgDB4|$devDeployStgDB4|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodDeployStgDB5|$devDeployStgDB5|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodDeployStgDB6|$devDeployStgDB6|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		echo "$prodDeployStgDB7|$devDeployStgDB7|T"  >> $OUTDIR/"$migProfile"_tpf_sp.list
		

		# Logic for Copying TPF Stored Procedures (Not Tested So Far)
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"A_SP"  -t ""$devView"A_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"B_SP"  -t ""$devView"B_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"C_SP"  -t ""$devView"C_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"D_SP"  -t ""$devView"D_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"E_SP"  -t ""$devView"E_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"F_SP"  -t ""$devView"F_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log
		$SCRIPTDIR/epdba_copy_structure.sh -a $TDSOURCE -b $TDTARGET -r $runName -s ""$prodView"A_SP"  -t ""$devView"G_SP" -w $ticketNo -o P -c 6 -m $OUTDIR/"$migProfile"_tpf_sp.list > $LOGDIR/archive/"$runName"_validate_envsetup.log

		updateTS=`date +%Y-%m-%d\ %H:%M:%S`
		echo "$updateTS : TPF Stored Procedures copied" >> $LOGDIR/archive/"$runName"_envsetup.log
			
	fi
		
		
#---------------------------------------------------------------------------------------------------------------------------------------------------------
	updateTS=`date +%Y-%m-%d\ %H:%M:%S`
	echo "$updateTS : Ending Environment Set Up for $runName !!" >> $LOGDIR/archive/"$runName"_envsetup.log
	