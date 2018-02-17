#!/usr/bin/ksh


# USAGE-1 Full Life Cycle		: ksh epdba_drive_metavalidation.sh -s PROD_MA -t STSTMA2 -m "MAS_Clarity_2014_IU0_IU1_Upgrade_Manifest_V1.5.dat" 
# USAGE-2 Load Manifest and Run report 	: ksh epdba_drive_metavalidation.sh -t STSTMA2 -m "MAS_Clarity_2014_IU0_IU1_Upgrade_Manifest_V1.5.dat"
# USAGE-3 Run the Report 		: ksh epdba_drive_metavalidation.sh -t DMDLNWV 
# USAGE-4 Load dbowner link 	: ksh epdba_drive_metavalidation.sh -t DMDLNWV  

#      For NC/SC
# USAGE-5 Full Life Cycle					 : ksh epdba_drive_metavalidation.sh -s PROD_SC -t RESC -m "SCAL_Clarity_2014_IU0_IU1_Upgrade_Manifest_V1.5.dat" -x "1|2|3|4|5|6"
# USAGE-6 Load Manifest and Run report 		 : ksh epdba_drive_metavalidation.sh -t RESC -m "SCAL_Clarity_2014_IU0_IU1_Upgrade_Manifest_V1.5.dat" -x "1|2|3|4|5|6"
# USAGE-7 Run the Report for all deployments : ksh epdba_drive_metavalidation.sh -t PROD_SC -x "1|2|3|4|5|6"
# USAGE-8 Run the Report for some deployments: ksh epdba_drive_metavalidation.sh -t PROD_SC -x "|2|||5|"



# STEP-1 Read Input Parameters

 	while getopts s:t:m:l:x: par
        do      case "$par" in
                s)    srcProfile="$OPTARG";;
                t)    tgtProfile="$OPTARG";;
				m)    manifestFile="$OPTARG";;
				l)    manifestLoadMethod="$OPTARG";;
				x)    deploymentDB="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh -s <sourceProfile> -t <tgtProfile> -m <manifestFile> -l <manifestLoadType> -d <dbownerLinkInd> -x 1|2|3|4|5|6 "
                        exit 998;;
                esac
        done

	
	if [ ! -z "$deploymentDB" ]
	then
		db1=`echo $deploymentDB | cut -f1 -d'|'`
		db2=`echo $deploymentDB | cut -f2 -d'|'`
		db3=`echo $deploymentDB | cut -f3 -d'|'`
		db4=`echo $deploymentDB | cut -f4 -d'|'`
		db5=`echo $deploymentDB | cut -f5 -d'|'`
		db6=`echo $deploymentDB | cut -f6 -d'|'`
	fi
	
	echo $manifestFile
	
# STEP-2 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi
		
	
	
# STEP-3 Create Log File

	scriptName=`basename $0`
	dateforlog=`date +%Y%m%d%H%M%S`
	logName=$scriptName-${dateforlog}.log
	logFileName=$LOGDIR/$logName


	echo "-------------------------------------------------------------------------------------" >> $logFileName
	echo "--------------- Starting Metadata Analysis at `date +%Y%m%d%H%M%S` -------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------" >> $logFileName
	


# STEP-4 Run Source and Target Profile Files

	if [ ! -z "$srcProfile" ]
	then
		echo "Running Source Regional Profile File - $srcProfile.profile " >> $logFileName
		USR_PROF=$HOMEDIR/region/$srcProfile.profile
		. $USR_PROF > /dev/null 2>&1
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
				echo "Profile file $srcProfile.profile cannot be found, Exiting" >> $logFileName
				exit 902
		else
			srcUshare=$ushareDB
			if [ ! -z "$TDDEV" ]
			then
				TDSRC="$TDDEV"
				TDDEV=""
				srcStgDB=$devStgDB
				srcRptDB=$devReportDB
				srcStgDB1=$devDeployStgDB1
				srcStgDB2=$devDeployStgDB2
				srcStgDB3=$devDeployStgDB3
				srcStgDB4=$devDeployStgDB4
				srcStgDB5=$devDeployStgDB5
				srcStgDB6=$devDeployStgDB6
				srcRptDB1=$devCalcReportDB1
				srcRptDB2=$devCalcReportDB2
				srcRptDB3=$devCalcReportDB3
				srcRptDB4=$devCalcReportDB4
				srcRptDB5=$devCalcReportDB5
				srcRptDB6=$devCalcReportDB6
				srcRptDB7=$devCalcReportDB7
				srcType="WITS"
			else
				if [ ! -z "$TDPROD" ]
				then
					TDSRC="$TDPROD"
					TDPROD=""
					srcStgDB=$prodStgDB
					srcRptDB=$prodReportDB
					srcStgDB1=$prodDeployStgDB1
					srcStgDB2=$prodDeployStgDB2
					srcStgDB3=$prodDeployStgDB3
					srcStgDB4=$prodDeployStgDB4
					srcStgDB5=$prodDeployStgDB5
					srcStgDB6=$prodDeployStgDB6
					srcRptDB1=$prodCalcReportDB1
					srcRptDB2=$prodCalcReportDB2
					srcRptDB3=$prodCalcReportDB3
					srcRptDB4=$prodCalcReportDB4
					srcRptDB5=$prodCalcReportDB5
					srcRptDB6=$prodCalcReportDB6
					srcRptDB7=$prodCalcReportDB7
					srcType="PROD"
				fi
			fi
		fi
	fi
	
	if [ ! -z "$tgtProfile" ]
	then
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
				TDTGT="$TDDEV"
				tgtStgDB=$devStgDB
				tgtRptDB=$devReportDB
				tgtStgDB1=$devDeployStgDB1
				tgtStgDB2=$devDeployStgDB2
				tgtStgDB3=$devDeployStgDB3
				tgtStgDB4=$devDeployStgDB4
				tgtStgDB5=$devDeployStgDB5
				tgtStgDB6=$devDeployStgDB6
				tgtRptDB1=$devCalcReportDB1
				tgtRptDB2=$devCalcReportDB2
				tgtRptDB3=$devCalcReportDB3
				tgtRptDB4=$devCalcReportDB4
				tgtRptDB5=$devCalcReportDB5
				tgtRptDB6=$devCalcReportDB6
				tgtRptDB7=$devCalcReportDB7
			else
			
				tgtType=`echo $tgtProfile | awk '{print substr($0,1,4)}'`
				TDTGT="$TDPROD"

				if [ "$tgtType" == "PROD"  ]
				then
					tgtStgDB=$prodStgDB
					tgtRptDB=$prodReportDB
					tgtStgDB1=$prodDeployStgDB1
					tgtStgDB2=$prodDeployStgDB2
					tgtStgDB3=$prodDeployStgDB3
					tgtStgDB4=$prodDeployStgDB4
					tgtStgDB5=$prodDeployStgDB5
					tgtStgDB6=$prodDeployStgDB6
					tgtRptDB1=$prodCalcReportDB1
					tgtRptDB2=$prodCalcReportDB2
					tgtRptDB3=$prodCalcReportDB3
					tgtRptDB4=$prodCalcReportDB4
					tgtRptDB5=$prodCalcReportDB5
					tgtRptDB6=$prodCalcReportDB6
					tgtRptDB7=$prodCalcReportDB7
				else
					tgtStgDB=$dryrunStgDB
					tgtRptDB=$dryrunReportDB
					tgtStgDB1=$dryrunDeployStgDB1
					tgtStgDB2=$dryrunDeployStgDB2
					tgtStgDB3=$dryrunDeployStgDB3
					tgtStgDB4=$dryrunDeployStgDB4
					tgtStgDB5=$dryrunDeployStgDB5
					tgtStgDB6=$dryrunDeployStgDB6
					tgtRptDB1=$dryrunCalcReportDB1
					tgtRptDB2=$dryrunCalcReportDB2
					tgtRptDB3=$dryrunCalcReportDB3
					tgtRptDB4=$dryrunCalcReportDB4
					tgtRptDB5=$dryrunCalcReportDB5
					tgtRptDB6=$dryrunCalcReportDB6
					tgtRptDB7=$dryrunCalcReportDB7
				fi
			fi
        fi
	fi
	
	
	echo "CALL CLARITY_DBA_MAINT.CLARITY_UPDATE_WITS_PROFILE ('$tgtProfile','','','',ResultStr)" > $TEMPDIR/clarity_update_wits_profile.sql
	$SCRIPTDIR/epdba_runSQLFile2.sh "tdd1.kp.org" $TEMPDIR/clarity_update_wits_profile.sql $TEMPDIR/clarity_update_wits_profile.out | tee -a  $logFileName
	
	currBaselineRegion=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f2 -d'|'`
	ticketNo=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f3 -d'|'`
	currmanifestFile=`tail -1 $TEMPDIR/clarity_update_wits_profile.out | cut -f4 -d'|'`



	
# STEP-5 Load dbowner link if required

		echo "Running SQL For dbowner link in Source ..." >> $logFileName
		rm -f $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
		if [ "$TDSRC" == "$TDDEV" ]
		then
			echo "DELETE FROM $ushareDB.UPGRADE_DB_OWNER_LINK" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
			echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$devownerId', '$devReportDB', '$devStgDB', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId1', '$devCalcReportDB1', '$devDeployStgDB1', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId2', '$devCalcReportDB2', '$devDeployStgDB2', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId3', '$devCalcReportDB3', '$devDeployStgDB3', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId4', '$devCalcReportDB4', '$devDeployStgDB4', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId5', '$devCalcReportDB5', '$devDeployStgDB5', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId6', '$devCalcReportDB6', '$devDeployStgDB6', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId7', '$devCalcReportDB7', '$devDeployStgDB7', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
			fi
		else
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g'  $METADATA/sqlQueries/load_dbowner_link.sql > $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
		fi
		
		if [ ! -z "$srcProfile" ]
		then
			$SCRIPTDIR/epdba_runSQLFile.sh $TDSRC $SQLDIR/"$tgtProfile"_load_dbowner_link.sql $TEMPDIR/"$tgtProfile"_load_dbowner_link.out | tee -a  $logFileName
		fi
		
	#----------------------------------------------------------------------------------------------------------------------------------------------------------	
		
		echo "Running SQL For dbowner link in Target ..." >> $logFileName
		rm -f $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
		if [ "$TDTGT" == "$TDDEV" ]
		then
			echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$devownerId', '$devReportDB', '$devStgDB', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId1', '$devCalcReportDB1', '$devDeployStgDB1', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId2', '$devCalcReportDB2', '$devDeployStgDB2', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId3', '$devCalcReportDB3', '$devDeployStgDB3', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId4', '$devCalcReportDB4', '$devDeployStgDB4', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId5', '$devCalcReportDB5', '$devDeployStgDB5', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId6', '$devCalcReportDB6', '$devDeployStgDB6', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
				echo "INSERT INTO $ushareDB.UPGRADE_DB_OWNER_LINK (cm_phy_owner_id, rpt_db, stg_db, _db, env) VALUES ('$deployId7', '$devCalcReportDB7', '$devDeployStgDB7', '$devView', 'WITS');" >> $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
			fi
		else
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g'  $METADATA/sqlQueries/load_dbowner_link.sql > $SQLDIR/"$tgtProfile"_load_dbowner_link.sql
		fi
		
		if [ ! -z "$tgtProfile" ]
		then
			$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $SQLDIR/"$tgtProfile"_load_dbowner_link.sql $TEMPDIR/"$tgtProfile"_load_dbowner_link.out | tee -a  $logFileName
		fi



	
	
# STEP-6 Load the manifest in the target server if a valid manifest file has been provided

	if [ ! -z "$manifestFile" ] #&& [ "$currmanifestFile" != "$manifestFile" ]
	then
	
		if [ ! -s $METADATA/manifest/"$manifestFile" ]
		then
			echo "Manifest File "$manifestFile" Not Found or is empty" >> $logFileName
			exit 906
		else
			# Remove Traling Spaces in each field
			perl -pi -e 's/\ *\|/\|/gi' $METADATA/manifest/"$manifestFile"
		fi
	
		if [ -z "$manifestLoadMethod" ]
		then
			# Default Option - Use INSERT Queries		
			rm -f $SQLDIR/manifest_load.sql
			cat $METADATA/manifest/"$manifestFile" | while read -r line ; 
			do	
				id=`echo $line | cut -f1 -d'|'`
				tableName=`echo $line | cut -f2 -d'|'`
				columnName=`echo $line | cut -f3 -d'|'`
				changeType=`echo $line | cut -f4 -d'|'`
				oldDatatype=`echo $line | cut -f5 -d'|'`
				newDatatype=`echo $line | cut -f6 -d'|'`
				testInd=`echo $line | cut -f7 -d'|'`

				echo "INSERT INTO $ushareDB.UPGRADE_MANIFEST_LOAD VALUES ($id,'$tableName','$columnName','$changeType','$oldDatatype','$newDatatype','$testInd');" >> $SQLDIR/manifest_load.sql
			done
		
		
			if [ -s $SQLDIR/manifest_load.sql ]
			then
				echo "DELETE FROM $ushareDB.UPGRADE_MANIFEST_LOAD ALL;" > $SQLDIR/cleanup_manifest_load.sql
				$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $SQLDIR/cleanup_manifest_load.sql $TEMPDIR/cleanup_manifest_load.out | tee -a  $logFileName
			fi
		
			$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDTGT 15 $SQLDIR manifest_load.sql $OUTDIR/manifest_load.out $logFileName	

		else
		
			# Option f - Use FASTLOAD
			manifestLoadMethod=`echo $manifestLoadMethod | tr '[a-z]' '[A-Z]'`
			if [ "$manifestLoadMethod" == "F" ]
			then
				USR_PROF=$HOME/user.profile
					. $USR_PROF > /dev/null 2>&1
					rt_cd=$?
					if [ $rt_cd -ne 0 ]
					then
							echo "Profile file user.profile cannot be found, Exiting" >> $logFileName
							exit 902
					fi

				case "$TDTGT" in
					"tdp1.kp.org") REPO=$RECP1
					;;
					"tdp2.kp.org") REPO=$RECP2
					;;
					"tdp3.kp.org") REPO=$RECP3
					;;
					"tdp5.kp.org") REPO=$RECP5
					;;
					"tdd1.kp.org") REPO=$RECD1
					;;
					"tdd3.kp.org") REPO=$RECD3
					;;
					"tdd4.kp.org") REPO=$RECD4
					;;
				esac


				echo ".LOGON $TDTGT/$USER,$REPO;"  > $FLOADDIR/"$tgtProfile"_manifest_load.fload
				sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_INFILE'/'$manifestFile'/g' $METADATA/sqlQueries/accdba_manifest_load.fload >> $FLOADDIR/"$tgtProfile"_manifest_load.fload
				$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o "$ushareDB.UPGRADE_MANIFEST_LOAD"  -f "$tgtProfile"_manifest_load.fload -l $logFileName
			else
				echo "Invalid Load Option for manifest file"
				exit 901
			fi
			
		fi
		
		sed -e 's/'MY_USHAREDB'/'$ushareDB'/g'  $METADATA/sqlQueries/load_manifest.sql > $SQLDIR/"$tgtProfile"_load_manifest.sql
		$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $SQLDIR/"$tgtProfile"_load_manifest.sql $TEMPDIR/"$tgtProfile"_load_manifest.out | tee -a  $logFileName

	else
		echo "Skipping Manifest Load ..." >> $logFileName
	fi


# STEP-7 Export Metadata from Source if source is specified (Source could be either WITS or PROD)
#		 If Source and Target are different boxes, then use combination of fload and fexp else use SELECT,INSERT

	if [ ! -z "$srcProfile" ]
	then

		# Export the entire metadata from the source

		rm -f $SQLDIR/"$tgtProfile"_export_roc_metadata.sql
		rm -f $SQLDIR/"$tgtProfile"_export_roc_metadata.out
		rm -f $SQLDIR/"$tgtProfile"_import_roc_metadata.sql
		rm -f $SQLDIR/"$tgtProfile"_import_roc_metadata.out

		srcType=`echo $srcProfile | awk '{print substr($0,1,4)}'`
		if [ "$srcType" == "PROD" ]
		then
        	sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_REPORTDB'/'$prodReportDB'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' \
			-e 's/'MY_CALCDB1'/'$prodCalcReportDB1'/g'  -e 's/'MY_CALCDB2'/'$prodCalcReportDB2'/g' -e 's/'MY_CALCDB3'/'$prodCalcReportDB3'/g' \
			-e 's/'MY_CALCDB4'/'$prodCalcReportDB4'/g' -e 's/'MY_CALCDB5'/'$prodCalcReportDB5'/g' -e 's/'MY_CALCDB6'/'$prodCalcReportDB6'/g' -e 's/'MY_CALCDB7'/'$prodCalcReportDB7'/g' \
			-e 's/'MY_DEPLOYDB1'/'$prodDeployStgDB1'/g'  -e 's/'MY_DEPLOYDB2'/'$prodDeployStgDB2'/g' -e 's/'MY_DEPLOYDB3'/'$prodDeployStgDB3'/g' \
			-e 's/'MY_DEPLOYDB4'/'$prodDeployStgDB4'/g' -e 's/'MY_DEPLOYDB5'/'$prodDeployStgDB5'/g' -e 's/'MY_DEPLOYDB6'/'$prodDeployStgDB6'/g' \
			-e 's/'MY_DEPLOYDB7'/'$prodDeployStgDB7'/g' -e 's/'MY_REGION'/'$region'/g' $METADATA/sqlQueries/export_roc_metadata.sql > $SQLDIR/"$tgtProfile"_export_roc_metadata.sql
			
		else
		    sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_STAGEDB'/'$devStgDB'/g' -e 's/'MY_REPORTDB'/'$devReportDB'/g' -e 's/'MY_USERDB'/'$devUserView'/g' \
			-e 's/'MY_CALCDB1'/'$devCalcReportDB1'/g'  -e 's/'MY_CALCDB2'/'$devCalcReportDB2'/g' -e 's/'MY_CALCDB3'/'$devCalcReportDB3'/g' \
			-e 's/'MY_CALCDB4'/'$devCalcReportDB4'/g' -e 's/'MY_CALCDB5'/'$devCalcReportDB5'/g' -e 's/'MY_CALCDB6'/'$devCalcReportDB6'/g' -e 's/'MY_CALCDB7'/'$devCalcReportDB7'/g' \
			-e 's/'MY_DEPLOYDB1'/'$devDeployStgDB1'/g'  -e 's/'MY_DEPLOYDB2'/'$devDeployStgDB2'/g' -e 's/'MY_DEPLOYDB3'/'$devDeployStgDB3'/g' \
			-e 's/'MY_DEPLOYDB4'/'$devDeployStgDB4'/g' -e 's/'MY_DEPLOYDB5'/'$devDeployStgDB5'/g' -e 's/'MY_DEPLOYDB6'/'$devDeployStgDB6'/g' \
			-e 's/'MY_DEPLOYDB7'/'$devDeployStgDB7'/g' -e 's/'MY_REGION'/'$region'/g'  $METADATA/sqlQueries/export_roc_metadata.sql > $SQLDIR/"$tgtProfile"_export_roc_metadata.sql
		fi
		
		
		$SCRIPTDIR/epdba_runSQLFile.sh $TDSRC $SQLDIR/"$tgtProfile"_export_roc_metadata.sql $TEMPDIR/"$tgtProfile"_export_roc_metadata.out | tee -a  $logFileName

		
		if [ "$TDSRC" != "$TDTGT" ]
		then
		
			# Cleanup Target USHARE Tables 
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' 	$METADATA/sqlQueries/import_roc_metadata.sql > $SQLDIR/"$tgtProfile"_import_roc_metadata.sql
			$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $SQLDIR/"$tgtProfile"_import_roc_metadata.sql $TEMPDIR/"$tgtProfile"_import_roc_metadata.out | tee -a  $logFileName

		
			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $srcUshare.UPGRADE_TABLES -d $IMPEXP/out_"$ushareDB".UPGRADE_TABLES.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $srcUshare.UPGRADE_COLUMNS -d $IMPEXP/out_"$ushareDB".UPGRADE_COLUMNS.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $srcUshare.UPGRADE_COLUMNS_VIEWS -d $IMPEXP/out_"$ushareDB".UPGRADE_COLUMNS_VIEWS.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $srcUshare.UPGRADE_STG_TBLS -d $IMPEXP/out_"$ushareDB".UPGRADE_STG_TBLS.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $srcUshare.UPGRADE_STG_COLUMNS -d $IMPEXP/out_"$ushareDB".UPGRADE_STG_COLUMNS.dat -l $logFileName


			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o $ushareDB.UPGRADE_TABLES -d $IMPEXP/out_"$ushareDB".UPGRADE_TABLES.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o $ushareDB.UPGRADE_COLUMNS -d $IMPEXP/out_"$ushareDB".UPGRADE_COLUMNS.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o $ushareDB.UPGRADE_COLUMNS_VIEWS -d $IMPEXP/out_"$ushareDB".UPGRADE_COLUMNS_VIEWS.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o $ushareDB.UPGRADE_STG_TBLS -d $IMPEXP/out_"$ushareDB".UPGRADE_STG_TBLS.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o $ushareDB.UPGRADE_STG_COLUMNS -d $IMPEXP/out_"$ushareDB".UPGRADE_STG_COLUMNS.dat -l $logFileName
		
		else
			# # If DRYRUN then move metadata to dryrun tables
			# tgtType=`echo $tgtProfile | awk '{print substr($0,1,6)}'`
			# if [ "$tgtType" == "DRYRUN" ]
			# then
				# echo "INSERT INTO HCCL"$region"_UPG_USHARE.UPGRADE_TABLES SELECT * FROM $ushareDB.UPGRADE_TABLES;" > $TEMPDIR/dryrun_dbc_tables.load.sql
				# echo "INSERT INTO HCCL"$region"_UPG_USHARE.UPGRADE_COLUMNS SELECT * FROM $ushareDB.UPGRADE_COLUMNS;" >> $TEMPDIR/dryrun_dbc_tables.load.sql
				# echo "INSERT INTO HCCL"$region"_UPG_USHARE.UPGRADE_COLUMNS_VIEWS SELECT * FROM $ushareDB.UPGRADE_COLUMNS_VIEWS;" >> $TEMPDIR/dryrun_dbc_tables.load.sql
				# echo "INSERT INTO HCCL"$region"_UPG_USHARE.UPGRADE_STG_TBLS SELECT * FROM $ushareDB.UPGRADE_STG_TBLS;" >> $TEMPDIR/dryrun_dbc_tables.load.sql
				# echo "INSERT INTO HCCL"$region"_UPG_USHARE.UPGRADE_STG_COLUMNS SELECT * FROM $ushareDB.UPGRADE_STG_COLUMNS;" >> $TEMPDIR/dryrun_dbc_tables.load.sql
				
				# $SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $TEMPDIR/dryrun_dbc_tables.load.sql $TEMPDIR/dryrun_dbc_tables.load.out | tee -a  $logFileName
			# else
				echo "Source same as Target. Skipping fastexport and fastload ... " >> $logFileName
			# fi
		fi
		
		
		# Replace Source DatabaseName With Target DatabaseName in USHARE UUPGRADE Tables. These will be compared with DBC Tables
		rm -f $SQLDIR/"$tgtProfile"_export_update_roc_metadata.sql
		rm -f $SQLDIR/"$tgtProfile"_export_update_roc_metadata.out
		if [ "$region" == "NC" ] || [ "$region" == "SC" ]
		then
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' -e 's/'MY_REPORT_DB1'/'$tgtRptDB1'/g'  -e 's/'MY_REPORT_DB2'/'$tgtRptDB2'/g'  -e 's/'MY_REPORT_DB3'/'$tgtRptDB3'/g'  \
			-e 's/'MY_REPORT_DB4'/'$tgtRptDB4'/g' -e 's/'MY_REPORT_DB5'/'$tgtRptDB5'/g' -e 's/'MY_REPORT_DB6'/'$tgtRptDB6'/g' -e 's/'MY_STAGE_DB1'/'$tgtStgDB1'/g'   \
			-e 's/'MY_STAGE_DB2'/'$tgtStgDB2'/g' -e 's/'MY_STAGE_DB3'/'$tgtStgDB3'/g' -e 's/'MY_STAGE_DB4'/'$tgtStgDB4'/g' -e 's/'MY_STAGE_DB5'/'$tgtStgDB5'/g'  \
			-e 's/'MY_STAGE_DB6'/'$tgtStgDB6'/g' -e 's/'MY_SOURCE_REPORT_DB1'/'$srcRptDB1'/g' -e 's/'MY_SOURCE_REPORT_DB2'/'$srcRptDB2'/g' -e 's/'MY_SOURCE_REPORT_DB3'/'$srcRptDB3'/g'   \
			-e 's/'MY_SOURCE_REPORT_DB4'/'$srcRptDB4'/g' -e 's/'MY_SOURCE_REPORT_DB5'/'$srcRptDB5'/g' -e 's/'MY_SOURCE_REPORT_DB6'/'$srcRptDB6'/g' -e 's/'MY_SOURCE_STAGE_DB1'/'$srcStgDB1'/g'  \
			-e 's/'MY_SOURCE_STAGE_DB2'/'$srcStgDB2'/g' -e 's/'MY_SOURCE_STAGE_DB3'/'$srcStgDB3'/g' -e 's/'MY_SOURCE_STAGE_DB4'/'$srcStgDB4'/g' -e 's/'MY_SOURCE_STAGE_DB5'/'$srcStgDB5'/g'   \
			-e 's/'MY_SOURCE_STAGE_DB6'/'$srcStgDB6'/g' -e 's/'MY_SOURCE_STAGE_DB7'/'$srcStgDB7'/g'  $METADATA/sqlQueries/export_update_ncsc_metadata.sql > $SQLDIR/"$tgtProfile"_export_update_roc_metadata.sql
		fi

		if [ ! -z "$tgtStgDB" ] && [ ! -z "$tgtRptDB" ]
		then
		
			echo "" >> $SQLDIR/"$tgtProfile"_export_update_roc_metadata.sql
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' -e 's/'MY_REPORT_DB'/'$tgtRptDB'/g'  -e 's/'MY_SOURCE_REPORT_DB'/'$srcRptDB'/g'  \
				-e 's/'MY_STAGE_DB'/'$tgtStgDB'/g' -e 's/'MY_SOURCE_STAGE_DB'/'$srcStgDB'/g' \
				$METADATA/sqlQueries/export_update_roc_metadata.sql >> $SQLDIR/"$tgtProfile"_export_update_roc_metadata.sql
			
			$SCRIPTDIR/epdba_runSQLFile.sh $TDTGT $SQLDIR/"$tgtProfile"_export_update_roc_metadata.sql $OUTDIR/"$tgtProfile"_export_update_roc_metadata.out | tee -a  $logFileName
		fi
		
	else
		echo "Skipping Export from Source Teradata Server ..." >> $logFileName

	fi

	
	
	
# STEP-8 Export the current differences in Production to WITS. (Needed only for Analysis of Validation Report)

	if [ ! -z "$srcProfile" ]
	then
		sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
		-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$devownerId'/g' \
		$METADATA/sqlQueries/export_current_issues_lead.sql > $SQLDIR/"$tgtProfile"_export_current_issues.sql
		
		if [ "$region" == "NC" ] || [ "$region" == "SC" ]
		then

			sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId1'/g' \
			-e 's/'MY_REPORT_DB'/'$prodCalcReportDB1'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB1'/g' -e 's/'MY_OFFSET'/'1000'/g' \
			$METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
			sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId2'/g' \
			-e 's/'MY_REPORT_DB'/'$prodCalcReportDB2'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB2'/g' -e 's/'MY_OFFSET'/'2000'/g' \
			$METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
			sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId3'/g' \
			-e 's/'MY_REPORT_DB'/'$prodCalcReportDB3'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB3'/g' -e 's/'MY_OFFSET'/'3000'/g' \
			$METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
			sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId4'/g' \
			-e 's/'MY_REPORT_DB'/'$prodCalcReportDB4'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB4'/g' -e 's/'MY_OFFSET'/'4000'/g' \
			$METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
			sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId5'/g' \
			-e 's/'MY_REPORT_DB'/'$prodCalcReportDB5'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB5'/g' -e 's/'MY_OFFSET'/'5000'/g' \
			$METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
			sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			-e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId6'/g' \
			-e 's/'MY_REPORT_DB'/'$prodCalcReportDB6'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB6'/g' -e 's/'MY_OFFSET'/'6000'/g' \
			$METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
			# sed -e 's/'MY_USHAREDB'/'$srcUshare'/g'  -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' \
			# -e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_OWNER_ID'/'$deployId7'/g' \
			# -e 's/'MY_REPORT_DB'/'$prodCalcReportDB7'/g' -e 's/'MY_STAGE_DB'/'$prodDeployStgDB7'/g' -e 's/'MY_OFFSET'/'7000'/g' \
			# $METADATA/sqlQueries/export_current_issues_deployment.sql >> $SQLDIR/"$tgtProfile"_export_current_issues.sql
			
		fi
		
		
		if [ "$srcType" == "PROD" ]
		then
			$SCRIPTDIR/epdba_runSQLFile.sh $TDSRC $SQLDIR/"$tgtProfile"_export_current_issues.sql $TEMPDIR/"$tgtProfile"_export_current_issues.out | tee -a  $logFileName

			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $srcUshare.UPGRADE_CURRENT_ISSUES -d $IMPEXP/out_"$ushareDB".UPGRADE_CURRENT_ISSUES.dat -l $logFileName
			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -o $ushareDB.UPGRADE_CURRENT_ISSUES -d $IMPEXP/out_"$ushareDB".UPGRADE_CURRENT_ISSUES.dat -l $logFileName
		fi
		
		
	fi	
	
	
	
	
	
# STEP-9 Update Current Manifest File and Baseline Region if different

	if [ "$currmanifestFile" != "$manifestFile" ] || [ "$srcProfile" != "$currBaselineRegion" ]
	then
		echo "CALL CLARITY_DBA_MAINT.CLARITY_UPDATE_WITS_PROFILE ('$tgtProfile','$srcProfile','','$manifestFile',ResultStr)" > $TEMPDIR/clarity_update_wits_profile.sql
		$SCRIPTDIR/epdba_runSQLFile2.sh "tdd1.kp.org" $TEMPDIR/clarity_update_wits_profile.sql $TEMPDIR/clarity_update_wits_profile.out | tee -a  $logFileName
		currBaselineRegion=$srcProfile
	fi
	
	

# Define Function
	create_validation_report()
	{
		
		VarStagingDB=$1
		VarReportingDB=$2
		VarDeploymentId=$3
		VarMatReportingDB=$4
		
		tgtType=`echo $tgtProfile | awk '{print substr($0,1,4)}'`
		
		if [ "$tgtType" == "PROD"  ]
		then
			env="PROD"				
			
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' -e 's/'MY_RUNNAME'/'$prodrunName'/g'  -e 's/'MY_ENV'/'$env'/g'  -e 's/'MY_OWNER_ID'/'$VarDeploymentId'/g' -e 's/'MY_NUID'/'$USER'/g' \
				-e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_DB'/'$prodView'/g' -e 's/'MY_REPORT_DB'/'$VarReportingDB'/g' -e 's/'MY_STAGE_DB'/'$VarStagingDB'/g' -e 's/'MY_MATVIEW_DB'/'$VarMatReportDB'/g'   \
				-e 's/'MY_LEAD_REPORTDB'/'$prodReportDB'/g' -e 's/'MY_LEAD_STAGEDB'/'$prodStgDB'/g' -e 's/'MY_DEPLOY_NAME'/'$prodReportDB'/g' -e 's/'MY_MATVIEW_DB'/'$VarMatReportingDB'/g' -e 's/'MY_MATVIEW_DB'/'$prodMatReportDB'/g' \
						$METADATA/sqlQueries/create_validation_report.sql > $SQLDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.sql

			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g'  -e 's/'MY_OWNER_ID'/'$VarDeploymentId'/g' -e 's/'MY_USERDB'/'$prodUserView'/g' -e 's/'MY_DB'/'$prodView'/g' \
			-e 's/'MY_REPORT_DB'/'$VarReportingDB'/g' -e 's/'MY_STAGE_DB'/'$VarStagingDB'/g'   \
			-e 's/'MY_MATVIEW_DB'/'$prodMatReportDB'/g' -e 's/'MY_KPBIVIEW_DB'/'$prodKPBIReportDB'/g' -e 's/'MY_ENV'/'$env'/g' -e 's/'MY_RUNNAME'/'$prodrunName'/g' \
			$METADATA/sqlQueries/analyze_validation_report.sql > $SQLDIR/"$tgtProfile"_"$VarReportingDB"_analyze_validation_report.sql				
						
			emailStatus="PROD Validation Completed"
			runName=$prodrunName

		fi
		
		
		if [ "$tgtType" == "DRYR"  ]
		then
			env="DRYRUN"				
			
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' -e 's/'MY_RUNNAME'/'$dryrunName'/g'  -e 's/'MY_ENV'/'$env'/g'  -e 's/'MY_OWNER_ID'/'$VarDeploymentId'/g' -e 's/'MY_NUID'/'$USER'/g' \
				-e 's/'MY_USERDB'/'$dryrunUserView'/g' -e 's/'MY_DB'/'$dryrunView'/g' -e 's/'MY_REPORT_DB'/'$VarReportingDB'/g' -e 's/'MY_STAGE_DB'/'$VarStagingDB'/g'  \
				-e 's/'MY_LEAD_REPORTDB'/'$dryrunReportDB'/g' -e 's/'MY_LEAD_STAGEDB'/'$dryrunStgDB'/g' -e 's/'MY_DEPLOY_NAME'/'$dryrunReportDB'/g' -e 's/'MY_MATVIEW_DB'/'$devMatReportDB'/g'  \
						$METADATA/sqlQueries/create_validation_report.sql > $SQLDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.sql

			emailStatus="DRYRUN Validation Completed"
			runName=$dryrunName

		fi

		if [ "$tgtType" != "PROD" ] && [ "$tgtType" != "DRYR" ]
		then
			env="WITS"
			
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g' -e 's/'MY_RUNNAME'/'$devrunName'/g'  -e 's/'MY_ENV'/'$env'/g'  -e 's/'MY_OWNER_ID'/'$VarDeploymentId'/g' -e 's/'MY_NUID'/'$USER'/g' \
				-e 's/'MY_USERDB'/'$devUserView'/g' -e 's/'MY_DB'/'$devView'/g' -e 's/'MY_REPORT_DB'/'$VarReportingDB'/g' -e 's/'MY_STAGE_DB'/'$VarStagingDB'/g'   \
				-e 's/'MY_LEAD_REPORTDB'/'$devReportDB'/g' -e 's/'MY_LEAD_STAGEDB'/'$devStgDB'/g' -e 's/'MY_DEPLOY_NAME'/'$devReportDB'/g' -e 's/'MY_MATVIEW_DB'/'$devMatReportDB'/g' \
						$METADATA/sqlQueries/create_validation_report.sql > $SQLDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.sql

						
			sed -e 's/'MY_USHAREDB'/'$ushareDB'/g'  -e 's/'MY_OWNER_ID'/'$VarDeploymentId'/g' -e 's/'MY_USERDB'/'$devUserView'/g' -e 's/'MY_DB'/'$devView'/g' \
			-e 's/'MY_REPORT_DB'/'$VarReportingDB'/g' -e 's/'MY_STAGE_DB'/'$VarStagingDB'/g'   \
			-e 's/'MY_MATVIEW_DB'/'$devMatReportDB'/g' -e 's/'MY_KPBIVIEW_DB'/'$devKPBIReportDB'/g' -e 's/'MY_ENV'/'$env'/g' -e 's/'MY_RUNNAME'/'$devrunName'/g' \
			$METADATA/sqlQueries/analyze_validation_report.sql > $SQLDIR/"$tgtProfile"_"$VarReportingDB"_analyze_validation_report.sql			
						
			emailStatus="WITS Validation Completed"
			runName=$devrunName
			
		fi

					# Create Validation Report
		rm -f $OUTDIR/"$runName"_validation_report_summary.dat
		rm -f $OUTDIR/"$runName"_validation_report_detail.dat
		rm -f $OUTDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.out
		$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $SQLDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.sql $OUTDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.out | tee -a  $logFileName

		#$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $SQLDIR/"$tgtProfile"_"$VarReportingDB"_analyze_validation_report.sql $OUTDIR/"$tgtProfile"_"$VarReportingDB"_analyze_validation_report.out | tee -a  $logFileName

		
		echo "runname,create_dttm,err_no,err_msg,dbname,erroring_dbname,stg_db,stg_table,tablename,columnname,is_deprecated,is_preserved,on_demand,is_extracted,data_retained,cm_phy_owner_id,Rpting Definition,Ushare or Staging Definition,Manifest Definition,slno,testing_rqrd" > $METADATA/reports/"$VarReportingDB"_validation_report_summary.csv
		sed '1d' $OUTDIR/"$runName"_validation_report_summary.dat | sed -e 's/\,/\;/g'  -e 's/|/\,/g' | sort -t',' -nk 3,3 -k 8,8 -k 9,9 >> $METADATA/reports/"$VarReportingDB"_validation_report_summary.csv
		
		echo "err_no,err_msg,dbname,erroring_dbname,stg_db,stg_table,tablename,columnname,is_deprecated,is_preserved,on_demand,is_extracted,data_retained,cm_phy_owner_id,Rpting Definition,Ushare or Staging Definition,Manifest Definition,slno,testing_rqrd" > $METADATA/reports/"$VarReportingDB"_validation_report_detail.csv
		sed '1d' $OUTDIR/"$runName"_validation_report_detail.dat | sed -e 's/\,/\;/g'  -e 's/|/\,/g' | sort -t',' -nk 1,1 -k 7,7 -k 8,8 -k 3,3 -k 4,4 -k 5,5 -k 6,6 >> $METADATA/reports/"$VarReportingDB"_validation_report_detail.csv

		#rm -f $OUTDIR/"$runName"_validation_report_summary.dat
		#rm -f $OUTDIR/"$runName"_validation_report_detail.dat
		
		echo "$METADATA/reports/|"$VarReportingDB"_validation_report_summary.csv" > $TEMPDIR/"$scriptName"_email_attach.dat
		echo "$METADATA/reports/|"$VarReportingDB"_validation_report_detail.csv" >> $TEMPDIR/"$scriptName"_email_attach.dat
		
		
							# Send Validation Report
		if [ "$region" == "NC" ] || [ "$region" == "SC" ]
		then
			emailSubjectLine="Report ran in $tgtProfile for $VarReportingDB" 
		else
			emailSubjectLine="Report ran in $tgtProfile"
		fi
	
	
		if [ -z "$manifestFile" ]
		then
			RptmanifestFile="$currmanifestFile"
		else
			RptmanifestFile="$manifestFile"
		fi
				
		echo "Hi, " > $TEMPDIR/"$scriptName"_email_body.dat
		echo "We have run the validation report. Report was generated by comparing objects in $tgtProfile with $currBaselineRegion." >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "The following Manifest was used - "$RptmanifestFile" " >> $TEMPDIR/"$scriptName"_email_body.dat
		echo ""  >> $TEMPDIR/"$scriptName"_email_body.dat
		echo ""  >> $TEMPDIR/"$scriptName"_email_body.dat
		echo ""  >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Environment Variables used were as follows - " >> $TEMPDIR/"$scriptName"_email_body.dat
		echo ""  >> $TEMPDIR/"$scriptName"_email_body.dat
		cat $OUTDIR/"$tgtProfile"_"$VarReportingDB"_create_validation_report.out >> $TEMPDIR/"$scriptName"_email_body.dat
		echo ""  >> $TEMPDIR/"$scriptName"_email_body.dat
		echo ""  >> $TEMPDIR/"$scriptName"_email_body.dat
		echo "Please find the validation report attached to this email." >> $TEMPDIR/"$scriptName"_email_body.dat

		$SCRIPTDIR/epdba_send_mail.sh -s "$emailStatus" -d "$emailSubjectLine" -b "$TEMPDIR/"$scriptName"_email_body.dat" -a "$TEMPDIR/"$scriptName"_email_attach.dat" -t "cd_bio_dba"
			
	}

	
	

# STEP-10 Run the metadata analysis in Target Teradata Server. Target could be either WITS or PROD

	rm -f $OUTDIR/"$tgtProfile"_create_validation_report.out
	rm -f $TEMPDIR/"$scriptName"_email_attach.dat
	if [ ! -z "$tgtProfile" ]
	then
		tgtType=`echo $tgtProfile | awk '{print substr($0,1,4)}'`
		
		
		echo "DROP TABLE $ushareDB.UPGRADE_ISSUES_REVIEW;
		CREATE TABLE $ushareDB.UPGRADE_ISSUES_REVIEW
		AS
		(
		SELECT A.*,
		CAST('' AS CHAR(1)) AS MANUAL_REVIEW_REQD,
		CAST('' AS VARCHAR(2500)) AS REVIEW_COMMENTS 
		FROM $ushareDB.UPGRADE_ISSUES A
		) WITH NO DATA
		PRIMARY INDEX (err_no ,dbname ,tablename ,columnname );" >  $TEMPDIR/create_upgrade_issues_review.sql
		
		$SCRIPTDIR/epdba_runSQLFile2.sh $TDTGT $TEMPDIR/create_upgrade_issues_review.sql $TEMPDIR/create_upgrade_issues_review.out | tee -a  $logFileName

		
		if [ "$tgtType" != "PROD"  ] && [ "$tgtType" != "DRYR" ]
		then
		
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				# Compare Mat View Report DB for lead reporting database in NC/SC
				create_validation_report $devStgDB $devReportDB $devownerId $prodMatReportDB
			else
				create_validation_report $devStgDB $devReportDB $devownerId
			fi
			
			
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				if [ ! -z "$db1" ]
				then
					VarStagingDB=$devDeployStgDB1
					VarReportingDB=$devCalcReportDB1
					VarDeploymentId=$deployId1
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db2" ]
				then
					VarStagingDB=$devDeployStgDB2
					VarReportingDB=$devCalcReportDB2
					VarDeploymentId=$deployId2
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db3" ]
				then
					VarStagingDB=$devDeployStgDB3
					VarReportingDB=$devCalcReportDB3
					VarDeploymentId=$deployId3
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db4" ]
				then
					VarStagingDB=$devDeployStgDB4
					VarReportingDB=$devCalcReportDB4
					VarDeploymentId=$deployId4
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db5" ]
				then
					VarStagingDB=$devDeployStgDB5
					VarReportingDB=$devCalcReportDB5
					VarDeploymentId=$deployId5
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db6" ]
				then
					VarStagingDB=$devDeployStgDB6
					VarReportingDB=$devCalcReportDB6
					VarDeploymentId=$deployId6
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db7" ]
				then
					VarStagingDB=$devDeployStgDB7
					VarReportingDB=$devCalcReportDB7
					VarDeploymentId=$deployId7
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
			fi
		fi
		
		if [ "$tgtType" == "PROD"  ] 
		then
			create_validation_report $prodStgDB $prodReportDB $devownerId
			
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				if [ ! -z "$db1" ]
				then
					VarStagingDB=$prodDeployStgDB1
					VarReportingDB=$prodCalcReportDB1
					VarDeploymentId=$deployId1
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db2" ]
				then
					VarStagingDB=$prodDeployStgDB2
					VarReportingDB=$prodCalcReportDB2
					VarDeploymentId=$deployId2
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db3" ]
				then
					VarStagingDB=$prodDeployStgDB3
					VarReportingDB=$prodCalcReportDB3
					VarDeploymentId=$deployId3
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db4" ]
				then
					VarStagingDB=$prodDeployStgDB4
					VarReportingDB=$prodCalcReportDB4
					VarDeploymentId=$deployId4
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db5" ]
				then
					VarStagingDB=$prodDeployStgDB5
					VarReportingDB=$prodCalcReportDB5
					VarDeploymentId=$deployId5
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db6" ]
				then
					VarStagingDB=$prodDeployStgDB6
					VarReportingDB=$prodCalcReportDB6
					VarDeploymentId=$deployId6
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db7" ]
				then
					VarStagingDB=$prodDeployStgDB7
					VarReportingDB=$prodCalcReportDB7
					VarDeploymentId=$deployId7
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
			fi
		fi
		
		
		if [ "$tgtType" == "DRYR" ]
		then
			create_validation_report $dryrunStgDB $dryrunReportDB $devownerId
			
			if [ "$region" == "NC" ] || [ "$region" == "SC" ]
			then
				if [ ! -z "$db1" ]
				then
					VarStagingDB=$dryrunDeployStgDB1
					VarReportingDB=$dryrunCalcReportDB1
					VarDeploymentId=$deployId1
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db2" ]
				then
					VarStagingDB=$dryrunDeployStgDB2
					VarReportingDB=$dryrunCalcReportDB2
					VarDeploymentId=$deployId2
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db3" ]
				then
					VarStagingDB=$dryrunDeployStgDB3
					VarReportingDB=$dryrunCalcReportDB3
					VarDeploymentId=$deployId3
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db4" ]
				then
					VarStagingDB=$dryrunDeployStgDB4
					VarReportingDB=$dryrunCalcReportDB4
					VarDeploymentId=$deployId4
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db5" ]
				then
					VarStagingDB=$dryrunDeployStgDB5
					VarReportingDB=$dryrunCalcReportDB5
					VarDeploymentId=$deployId5
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db6" ]
				then
					VarStagingDB=$dryrunDeployStgDB6
					VarReportingDB=$dryrunCalcReportDB6
					VarDeploymentId=$deployId6
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
				if [ ! -z "$db7" ]
				then
					VarStagingDB=$dryrunDeployStgDB7
					VarReportingDB=$dryrunCalcReportDB7
					VarDeploymentId=$deployId7
					create_validation_report $VarStagingDB $VarReportingDB $VarDeploymentId
				fi
			fi
		fi
		
	fi

	
	echo "-------------------------------------------------------------------------------------" >> $logFileName
	echo "--------------- Ending Metadata Analysis at `date +%Y%m%d%H%M%S` -------------------" >> $logFileName
	echo "--------------------------------------------------------------------------------------" >> $logFileName






