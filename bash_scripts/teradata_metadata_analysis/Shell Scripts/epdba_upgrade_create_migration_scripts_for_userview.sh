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
		


#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-3 Get USER View DDL from WITS. Convert it to Prod Definition


	#---------------------------------------------------------------------------------------------------
	#------------------------- Creation of User Views for Reporting Tables -----------------------------
	#---------------------------------------------------------------------------------------------------

	rm -f $SQLDIR/"$relName"_cutover_tpf_userview.sql
	rm -f $SQLDIR/"$relName"_cutover_prod_userview_*.sql
	rm -f $SQLDIR/"$relName"_validation_userview_access.sql
	
	# rm -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql

	
	rm -f $SQLDIR/"$relName"_wits_userview_ddl.sql
	rm -f $SQLDIR/"$relName"_prod_userview_ddl.sql
	rm -f $TEMPDIR/"$relName"_wits_userview_*.out
	rm -f $TEMPDIR/"$relName"_prod_userview_*.out
	
	
	
	countValue="0"
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND VIEW ENTRY" | cut -f2,4 -d'|' | sort | uniq | while read -r line2 ; do

		table=`echo $line2 | cut -f1 -d'|'`
		prodDB=`echo $line2 | cut -f2 -d'|'`
	
		countValue=`expr $countValue + 1`
	
	
		cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$devUserView'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
		-e 's/'MY_OUTDDL_FILE'/'"$relName"_wits_userview_"$table"\.out'/g' -e 's/'MY_OBJECT'/'VIEW'/g'  >> $SQLDIR/"$relName"_wits_userview_ddl.sql
		
		cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$prodDB'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
		-e 's/'MY_OUTDDL_FILE'/'"$relName"_prod_userview_"$table"\.out'/g' -e 's/'MY_OBJECT'/'VIEW'/g'  >> $SQLDIR/"$relName"_prod_userview_ddl.sql
	

	done
	

	# Extract WITS DDL for  VIEWS
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_wits_userview_ddl.sql $TEMPDIR/"$scriptName"_witsddl.out | tee -a  $logFileName

	# Extract PROD DDL for  VIEWS
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/"$relName"_prod_userview_ddl.sql $TEMPDIR/"$scriptName"_prodddl.out | tee -a  $logFileName
	
	
	rm -f $TEMPDIR/"userview"_check_prod_custom_view.out
	echo "SELECT TRIM(A.TableName) AS RESTRICTED_TABLE from dbc.tablesv A JOIN CLARITY_DBA_MAINT.CLARITY_UPG_TABLES_CHG_CTGRY B ON B.RUN_ID=$runId AND TRIM(A.TableName)=TRIM(B.UPG_TABLE_NAME)" > $TEMPDIR/"userview"_check_prod_custom_view.sql
	echo " WHERE A.databasename='$prodUserView'  AND (A.RequestText LIKE '%ROLE%' OR A.requesttext not like '%SEL%*%') GROUP BY 1" >> $TEMPDIR/"userview"_check_prod_custom_view.sql
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"userview"_check_prod_custom_view.sql $TEMPDIR/"userview"_check_prod_custom_view.out | tee -a  $logFileName
	

	

	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND VIEW ENTRY" | cut -f2,4 -d'|' | sort | uniq | while read -r line2 ; do

		table=`echo $line2 | cut -f1 -d'|'`
		prodDB=`echo $line2 | cut -f2 -d'|'`
		
		
		echo "SELECT \\* FROM $prodUserView.\"$table\" SAMPLE 1;" >> $SQLDIR/"$relName"_validation_userview_access.sql
		
		
		rm -f $TEMPDIR/"userview"_"TDDEV"_"$table".out
		rm -f $TEMPDIR/"userview"_"TDPROD"_"$table".out

		cat $TEMPDIR/"$relName"_wits_userview_"$table".out > $TEMPDIR/"userview"_"TDDEV"_"$table".out 
		cat $TEMPDIR/"$relName"_prod_userview_"$table".out > $TEMPDIR/"userview"_"TDPROD"_"$table".out 
		cat $DIR/$dbChgList | grep -w -i $table > $TEMPDIR/"userview"_"$table"_manifest_changes.out | tee -a  $logFileName
		

		
		# Check if User View Exisits in WITS with Data Restrictions
		if [ -s $TEMPDIR/"userview"_"TDDEV"_"$table".out ]
		then
			cnt1=`cat $TEMPDIR/"userview"_"TDDEV"_"$table".out | grep -i -w "UNION" | wc -l`
			cnt2=`cat $TEMPDIR/"userview"_"TDDEV"_"$table".out | grep -i -w "JOIN" | wc -l`
			cnt3=`cat $TEMPDIR/"userview"_"TDDEV"_"$table".out | grep -i -w "ROLE" | wc -l`
			witsdataResInd1=`expr $cnt1 + $cnt2 + $cnt3`
		else
			witsdataResInd1="0"
		fi
		
		
		# Check if User View Exisits in PROD with Data Restrictions
		if [ -s $TEMPDIR/"userview"_"TDPROD"_"$table".out ]
		then
			cnt1=`cat $TEMPDIR/"userview"_"TDPROD"_"$table".out | grep -i -w "UNION" | wc -l`
			cnt2=`cat $TEMPDIR/"userview"_"TDPROD"_"$table".out | grep -i -w "JOIN" | wc -l`
			cnt3=`cat $TEMPDIR/"userview"_"TDPROD"_"$table".out | grep -i -w "ROLE" | wc -l`
			dataResInd1=`expr $cnt1 + $cnt2 + $cnt3`
		else
			dataResInd1="0"
		fi
		

		
		
		if [ -s $TEMPDIR/"userview"_"TDPROD"_"$table".out ]
		then

		
			# Determine Data Restriction in USER VIEW (Check-2)			
			dataResInd2=`grep -i -w "$table" $TEMPDIR/"userview"_check_prod_custom_view.out | wc -l`		
		
		
			if [ $dataResInd1 -ne 0 ] || [ $dataResInd2 -ne 0 ]
			then

				prodSelectInd=`cat $TEMPDIR/"userview"_"TDPROD"_"$table".out  | grep -i 'SEL' | grep -i '\*' | wc -l`
				if [ "$prodSelectInd" -eq 0 ]
				then
			
					# User View Exisits in Production with Data Restrictions
					# Add the new columns to the view definition


					grep -w -n "FROM" $TEMPDIR/"userview"_"TDPROD"_"$table".out | grep -v '\-\-' | cut -f1 -d':' > $TEMPDIR/"userview"_"TDPROD"_"$table"_prodLine.out
					fileLength=`cat $TEMPDIR/"userview"_"TDPROD"_"$table".out | wc -l`


					head -1 $TEMPDIR/"userview"_"TDPROD"_"$table"_prodLine.out | sort | uniq | while read -r line2 ; do

						headLine=`expr $line2 - 1`
						tailLine=`expr $fileLength - $line2 + 1`
						head -$headLine $TEMPDIR/"userview"_"TDPROD"_"$table".out > $TEMPDIR/"userview"_"TDPROD"_"$table"_userview.tmp

						cat $DIR/$dbChgList | grep -w -i $table | cut -f3 -d'|'  > $TEMPDIR/"userview"_getcols.tmp

						headerAdded="F"
						cat $TEMPDIR/"userview"_getcols.tmp | while read -r fieldName; do
						
							foundInd=`cat $TEMPDIR/"userview"_"TDPROD"_"$table".out | grep -i -w "$fieldName" | wc -l`
							if [ $foundInd -eq 0 ] && [ "$headerAdded" == "F" ]
							then
								echo "/* Addition of Columns in WITS */" >> $TEMPDIR/"userview"_"TDPROD"_"$table"_userview.tmp
								headerAdded="T"
							fi
							if [ $foundInd -eq 0 ]
							then
								echo ",$fieldName"  >> $TEMPDIR/"userview"_"TDPROD"_"$table"_userview.tmp
							fi
						done


						tail -$tailLine $TEMPDIR/"userview"_"TDPROD"_"$table".out >> $TEMPDIR/"userview"_"TDPROD"_"$table"_userview.tmp
						mv $TEMPDIR/"userview"_"TDPROD"_"$table"_userview.tmp $TEMPDIR/"userview"_"TDPROD"_"$table".out

					done
					
					
					cat $TEMPDIR/"userview"_"TDPROD"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql

					# Review Cases where PROD custom view might miss out on a change in WITS
					if [ "$dataResInd1" -ne "$witsdataResInd1" ]
					then
						echo "$table : Mismatch between WITS $witsdataResInd1 and PROD $dataResInd1 in User View. ROLE,UNION,JOIN Count is not the same" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
					fi
					
				else
					
					# User View Exisits in Production without Data Restrictions but is a SELECT * VIEW
					# Refresh current view in production provided the view in WITS is not a CUSTOM View
					
					
					# Check if User View Exisits in WITS with Data Restrictions
					# Determine Data Restriction in USER VIEW (Check-1)		
					
					if [ $witsdataResInd1 -gt 0 ] 
					then
						
						witsSelectInd=`cat $TEMPDIR/"userview"_"TDDEV"_"$table".out  | grep -i 'SEL' | grep -i '\*' | wc -l`
						if [ "$witsSelectInd" -eq 0 ]
						then
							# Copy from WITS
							sed -e 's/'$devView'/'$prodView'/g'  -e 's/'$devUserView'/'$prodUserView'/g' \
							$TEMPDIR/"userview"_"TDDEV"_"$table".out  >> $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
							
							# Review Cases where WITS custom view might override a change in PROD
							if [ "$dataResInd1" -ne "$witsdataResInd1" ]
							then
								echo "$table : Mismatch between WITS $witsdataResInd1 and PROD $dataResInd1 in User View. ROLE,UNION,JOIN Count is not the same" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
							fi
							
						else
							# Refresh current view in production, since view in WITS has a DR but is also a SELECT *
							cat $TEMPDIR/"userview"_"TDPROD"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql
						fi
					
					else
					
						# User View Exisits in both WITS and Production without Data Restrictions
						# Refresh current view in production
						cat $TEMPDIR/"userview"_"TDPROD"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql
					fi
					
				fi
				
			else

				# Check if User View Exisits in WITS with Data Restrictions
				if [ $witsdataResInd1 -gt 0 ] 
				then
					sed -e 's/'$devView'/'$prodView'/g'  -e 's/'$devUserView'/'$prodUserView'/g' \
						$TEMPDIR/"userview"_"TDDEV"_"$table".out  >> $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
				else
					# Refresh User View from PROD without Data Restrictions
					cat $TEMPDIR/"userview"_"TDPROD"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql
				fi
			fi

			
			#--------------------------------  Logic for Existing TPF Tables ----------------------------#
            # Refresh User View For Exisiting TPF Tables
			tpfInd2=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY"  | grep -i -w $table | wc -l`
			if [ $tpfInd2 -ne 0 ] && [ -z "$prodNoDummyUserView" ]
			then
				# sed -e 's/'$prodView'/'$prodTpfView'/g'  -e 's/'$prodUserView'/'$prodTpfUserView'/g' \
					# -e 's/'$devView'/'$prodTpfView'/g'  -e 's/'$devUserView'/'$prodTpfUserView'/g' \
				# $TEMPDIR/"userview"_"TDPROD"_"$table".out > $TEMPDIR/"userview"_userview_tpftable.out
			
				sed -e 's/'MY_TGT_DB'/'$prodTpfUserView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
					-e 's/'MY_SRC_DB'/'$prodTpfView'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
					> $TEMPDIR/"userview"_useriew_tpftable.out
			fi
			
		else
		
		# User View does not exist in Production
		
			if [ -s $TEMPDIR/"userview"_"TDDEV"_"$table".out ]
			then
			
				# Check if User View Exisits in WITS with Data Restrictions
				# Determine Data Restriction in USER VIEW (Check-1)			
				if [ $witsdataResInd1 -ne 0 ] 
				then
					sed -e 's/'$devView'/'$prodView'/g'  -e 's/'$devUserView'/'$prodUserView'/g' \
 					$TEMPDIR/"userview"_"TDDEV"_"$table".out  >> $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
			
				else			
					# User View Exisits in both WITS and Production without Data Restrictions
					# Copy current view from WITS without restrictions
					
					sed -e 's/'$devView'/'$prodView'/g'  -e 's/'$devUserView'/'$prodUserView'/g' \
 					$TEMPDIR/"userview"_"TDDEV"_"$table".out  >> $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql
				fi
				

			else
				# User View does not exist in WITS and Prod !!
				# Create a new view as SELECT * FROM <VIEW>

				sed -e 's/'MY_TGT_DB'/'$prodUserView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
				-e 's/'MY_SRC_DB'/'$prodView'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
				>> $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql
				
				
				#--------------------------------  Logic for TPF Tables ----------------------------#
							# Add User View For New TPF Tables
				tpfInd2=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY"  | grep -i -w $table | wc -l`
				if [ $tpfInd2 -ne 0 ]
				then
					sed -e 's/'MY_TGT_DB'/'$prodTpfUserView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
					-e 's/'MY_SRC_DB'/'$prodTpfView'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
					> $TEMPDIR/"userview"_useriew_tpftable.out
				fi	
				
			fi
		fi


		tpfInd2=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | wc -l`
		if [ $tpfInd2 -ne 0 ]
		then
			cat $TEMPDIR/"userview"_userview_tpftable.out >> $SQLDIR/"$relName"_cutover_tpf_userview.sql
			echo "SELECT \\* FROM $prodTpfUserView.\"$table\" SAMPLE 1;" >> $SQLDIR/"$relName"_validation_userview_access.sql
		fi


		rm -f $TEMPDIR/"userview"_"TDPROD"_"$table"_prodLine.out
		rm -f $TEMPDIR/"userview"_"TDDEV"_"$table".out
		rm -f $TEMPDIR/"userview"_"TDPROD"_"$table".out
		rm -f $TEMPDIR/"userview"_"$table"_manifest_changes.out

	done

	
	
	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_refresh_with_dr.sql
		mv $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql $ticketNo 
		rm -f $TEMPDIR/"userview"_"$relName"_cutover_prod_userview_refresh_with_dr.sql
		
		rm -f $TEMPDIR/java_results.out
		cd $JAVADIR
		java -DoutputFile="$TEMPDIR/java_results.out" -DinputFile="$SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql" -DreasonText="$customViewPurpose" CustomViewFormatter
		rm -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr_final.sql
		mv $TEMPDIR/java_results.out $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr_final.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_refresh_without_dr.sql
		mv $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql $ticketNo 
		rm -f $TEMPDIR/"userview"_"$relName"_cutover_prod_userview_refresh_without_dr.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql
		mv $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql $ticketNo 
		rm -f $TEMPDIR/"userview"_"$relName"_cutover_prod_userview_copy_from_wits_without_dr.sql
	fi
	
	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
		mv $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql $ticketNo 
		rm -f $TEMPDIR/"userview"_"$relName"_cutover_prod_userview_copy_from_wits_with_dr.sql
	fi
	
	if [ -f $SQLDIR/"$relName"_cutover_tpf_userview.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_tpf_userview.sql
		mv $SQLDIR/"$relName"_cutover_tpf_userview.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_tpf_userview.sql $SQLDIR/"$relName"_cutover_tpf_userview.sql $ticketNo 
		rm -f $TEMPDIR/"userview"_"$relName"_cutover_tpf_userview.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_create_new.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql
	fi
	
	