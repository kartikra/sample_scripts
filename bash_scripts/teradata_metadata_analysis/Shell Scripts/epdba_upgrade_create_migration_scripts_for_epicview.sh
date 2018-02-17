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
# STEP-3 Get  View DDL from WITS. Convert it to Prod Definition


	#---------------------------------------------------------------------------------------------------
	#------------------------- Creation of  Views for Reporting Tables -----------------------------
	#---------------------------------------------------------------------------------------------------

	rm -f $SQLDIR/"$relName"_cutover_prod_view_*.sql
	rm -f $SQLDIR/"$relName"_cutover_tpf_view.sql

	# rm -f $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_view_create_new.sql
	# rm -f $SQLDIR/"$relName"_cutover_prod_view_materialized.sql

	
	rm -f $SQLDIR/"$relName"_wits_view_ddl.sql
	rm -f $SQLDIR/"$relName"_prod_view_ddl.sql
	rm -f $TEMPDIR/"$relName"_wits_view_*.out
	rm -f $TEMPDIR/"$relName"_prod_view_*.out
	
	
	
	countValue="0"
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND VIEW ENTRY" | cut -f2,3 -d'|' | sort | uniq | while read -r line2 ; do

		table=`echo $line2 | cut -f1 -d'|'`
		prodDB=`echo $line2 | cut -f2 -d'|'`
	
		countValue=`expr $countValue + 1`
	
	
		cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$devView'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
		-e 's/'MY_OUTDDL_FILE'/'"$relName"_wits_view_"$devView"_"$table"\.out'/g' -e 's/'MY_OBJECT'/'VIEW'/g'  >> $SQLDIR/"$relName"_wits_view_ddl.sql
		
		cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$prodDB'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
		-e 's/'MY_OUTDDL_FILE'/'"$relName"_prod_view_"$prodDB"_"$table"\.out'/g' -e 's/'MY_OBJECT'/'VIEW'/g'  >> $SQLDIR/"$relName"_prod_view_ddl.sql
	

	done
	

	# Extract WITS DDL for  VIEWS
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_wits_view_ddl.sql $TEMPDIR/"$scriptName"_witsddl.out | tee -a  $logFileName

	# Extract PROD DDL for  VIEWS
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/"$relName"_prod_view_ddl.sql $TEMPDIR/"$scriptName"_prodddl.out | tee -a  $logFileName
	
	
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND VIEW ENTRY" | cut -f2,3 -d'|' | sort | uniq | while read -r line2 ; do

		table=`echo $line2 | cut -f1 -d'|'`
		prodDB=`echo $line2 | cut -f2 -d'|'`

		
		rm -f $TEMPDIR/"view"_"TDDEV"_"$table".out
		rm -f $TEMPDIR/"view"_"TDPROD"_"$table".out

		if [ "$prodDB" == "$prodView" ] || [ -z "$prodDB" ]
		then
			# Exisiting  View (non-Materialized) or New  View being copied from WITS
			
			cat $TEMPDIR/"$relName"_wits_view_"$devView"_"$table".out > $TEMPDIR/"view"_"TDDEV"_"$table".out 
			cat $TEMPDIR/"$relName"_prod_view_"$prodDB"_"$table".out > $TEMPDIR/"view"_"TDPROD"_"$table".out 
			
			cat $DIR/$dbChgList | grep -w -i $table > $TEMPDIR/"view"_"$table"_manifest_changes.out | tee -a  $logFileName

			if [ -s $TEMPDIR/"view"_"TDDEV"_"$table".out ]
			then
				#  View exist in WITS. Copy to Prod

				sed -e 's/'$devReportDB'/'$prodReportDB'/g'  -e 's/'$devView'/'$prodView'/g' \
				$TEMPDIR/"view"_"TDDEV"_"$table".out  >> $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql

				
				# Determine Complexity of  VIEW			
				# rm -f $TEMPDIR/"view"_check_wits_complex_view.out
				# rm -f $TEMPDIR/"view"_check_prod_complex_view.out
				# echo "SELECT TRIM(TableName) from dbc.tablesv where databasename='$devView' and TRIM(TableName)='$table' and requesttext not like '%SEL%*%'" > $TEMPDIR/"view"_check_wits_complex_view.sql
				# echo "SELECT TRIM(TableName) from dbc.tablesv where databasename='$prodView' and TRIM(TableName)='$table' and requesttext not like '%SEL%*%'" > $TEMPDIR/"view"_check_prod_complex_view.sql
				
				# $SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"view"_check_wits_complex_view.sql $TEMPDIR/"view"_check_wits_complex_view.out | tee -a  $logFileName
				# $SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"view"_check_prod_complex_view.sql $TEMPDIR/"view"_check_prod_complex_view.out | tee -a  $logFileName
				
				
				flag1=`egrep -i "UNION|JOIN" $TEMPDIR/"view"_"TDDEV"_"$table".out | wc -l`		
				flag2=`egrep -i "UNION|JOIN" $TEMPDIR/"view"_"TDPROD"_"$table".out | wc -l`
				
				
				if [ "$flag1" -ne 0 ]  
				then
					if [ "$flag2" -ne 0 ]
					then
						echo "Currently view in PROD is not complex.Complex  View being moved to PROD for $table from $devView to $prodView." >> $SQLDIR/"$relName"_cutover_prod_view_complexity_alert.sql
					else
						echo "Current view in PROD is also complex. Complex  View being moved to PROD for $table from $devView to $prodView. Validate if there is any change in logic" >> $SQLDIR/"$relName"_cutover_prod_view_complexity_alert.sql
					fi
				fi
				
				
				#--------------------------------  Logic for TPF Tables ----------------------------#
							# Add  View For Existing TPF Tables
				if [ -z "$prodNoDummyView" ]
				then
					sed -e 's/'$devReportDB'/'$prodTpfReportDB'/g'  -e 's/'$devView'/'$prodTpfView'/g' \
					$TEMPDIR/"view"_"TDDEV"_"$table".out  > $TEMPDIR/"view"_view_tpftable.out
				fi
				
			else
			
				# Not Found in WITS. Refresh from Production
				
				if [ -s $TEMPDIR/"view"_"TDPROD"_"$table".out ]
				then
					dataResInd=`grep -w -n "ROLE IN" $TEMPDIR/"view"_"TDPROD"_"$table".out | wc -l`
			
					# Refresh  View in Production
			
					if [ $dataResInd -ne 0 ]
					then

						#  View Exisits in Production with Data Restrictions
						# Add the new columns to the view definition

						grep -w -n "FROM" $TEMPDIR/"view"_"TDPROD"_"$table".out | grep -v '\-\-' | cut -f1 -d':' > $TEMPDIR/"view"_"TDPROD"_"$table"_prodLine.out
						fileLength=`cat $TEMPDIR/"view"_"TDPROD"_"$table".out | wc -l`


						head -1 $TEMPDIR/"view"_"TDPROD"_"$table"_prodLine.out | sort | uniq | while read -r line2 ; do

							headLine=`expr $line2 - 1`
							tailLine=`expr $fileLength - $line2 + 1`
							head -$headLine $TEMPDIR/"view"_"TDPROD"_"$table".out > $TEMPDIR/"view"_"TDPROD"_"$table"_view.tmp

							cat $DIR/$dbChgList | grep -w -i $table | cut -f3 -d'|'  > $TEMPDIR/"view"_getcols.tmp

							headerAdded="F"
							cat $TEMPDIR/"view"_getcols.tmp | while read -r fieldName; do

								foundInd=`cat $TEMPDIR/"view"_"TDPROD"_"$table".out | grep -i -w "$fieldName" | wc -l`
								if [ $foundInd -eq 0 ] && [ "$headerAdded" == "F" ]
								then
									echo "/* Addition of Columns in WITS */" >> $TEMPDIR/"view"_"TDPROD"_"$table"_view.tmp
									headerAdded="T"
								fi
								
								if [ $foundInd -eq 0 ]
								then
									echo ",\"$fieldName\""  >> $TEMPDIR/"view"_"TDPROD"_"$table"_view.tmp
								fi

							done


							tail -$tailLine $TEMPDIR/"view"_"TDPROD"_"$table".out >> $TEMPDIR/"view"_"TDPROD"_"$table"_view.tmp
							mv $TEMPDIR/"view"_"TDPROD"_"$table"_view.tmp $TEMPDIR/"view"_"TDPROD"_"$table".out

						done

						cat $TEMPDIR/"view"_"TDPROD"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql


					else

						#  View Exisits in Production without Data Restrictions
						# Refresh current view in production

						cat $TEMPDIR/"view"_"TDPROD"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql

					fi
					
					#--------------------------------  Logic for TPF Tables ----------------------------#
							# Add  View For Existing TPF Tables
					tpfInd=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY"  | grep -i -w $table | wc -l`
					if [ $tpfInd -ne 0 ] && [ -z "$prodNoDummyView" ]
					then
						sed -e 's/'$devReportDB'/'$prodTpfReportDB'/g'  -e 's/'$devView'/'$prodTpfView'/g' \
							$TEMPDIR/"view"_"TDPROD"_"$table".out > $TEMPDIR/"view"_view_tpftable.out
					fi
		 
				else
				
					#  View does not exist in WITS and Prod !!
					# Create a new view as SELECT * FROM <Reporting Table>

					sed -e 's/'MY_TGT_DB'/'$prodView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
					-e 's/'MY_SRC_DB'/'$prodReportDB'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
					>> $SQLDIR/"$relName"_cutover_prod_view_create_new.sql
				
				
					#--------------------------------  Logic for TPF Tables ----------------------------#
								# Add  View For New TPF Tables
					tpfInd=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY"  | grep -i -w $table | wc -l`
					if [ $tpfInd -ne 0 ]
					then
						sed -e 's/'MY_TGT_DB'/'$prodTpfView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
						-e 's/'MY_SRC_DB'/'$prodTpfReportDB'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
						> $TEMPDIR/"view"_view_tpftable.out
					fi		
				fi
			fi

			tpfInd=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | wc -l`
			if [ $tpfInd -ne 0 ]
			then
				cat $TEMPDIR/"view"_view_tpftable.out >> $SQLDIR/"$relName"_cutover_tpf_view.sql
			fi

		else
			# Materialized  View
			cat $TEMPDIR/"$relName"_prod_view_"$prodDB"_"$table".out >> $SQLDIR/"$relName"_cutover_prod_view_materialized.sql
		fi
		
		rm -f $TEMPDIR/"view"_"TDPROD"_"$table"_prodLine.out
		rm -f $TEMPDIR/"view"_"TDDEV"_"$table".out
		rm -f $TEMPDIR/"view"_"TDPROD"_"$table".out
		rm -f $TEMPDIR/"view"_"$table"_manifest_changes.out

	done


	if [ -f $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_refresh_with_dr.sql
		mv $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql $ticketNo dummy.txt 1
		rm -f $TEMPDIR/"view"_"$relName"_cutover_prod_view_refresh_with_dr.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_refresh_without_dr.sql
		mv $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql $ticketNo dummy.txt 1 
		rm -f $TEMPDIR/"view"_"$relName"_cutover_prod_view_refresh_without_dr.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_copy_from_wits.sql
		mv $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_view_copy_from_wits.sql $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql $ticketNo dummy.txt 1 
		rm -f $TEMPDIR/"view"_"$relName"_cutover_prod_view_copy_from_wits.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_tpf_view.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_tpf_view.sql
		mv $SQLDIR/"$relName"_cutover_tpf_view.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_tpf_view.sql $SQLDIR/"$relName"_cutover_tpf_view.sql $ticketNo dummy.txt 1 
		rm -f $TEMPDIR/"view"_"$relName"_cutover_tpf_view.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_view_materialized.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_materialized.sql
		mv $SQLDIR/"$relName"_cutover_prod_view_materialized.sql $TEMPDIR
		$SCRIPTDIR/epdba_create_target_view.sh $TEMPDIR/"$relName"_cutover_prod_view_materialized.sql $SQLDIR/"$relName"_cutover_prod_view_materialized.sql $ticketNo dummy.txt 1 
		rm -f $TEMPDIR/"view"_"$relName"_cutover_prod_view_materialized.sql
	fi
	
	if [ -f $SQLDIR/"$relName"_cutover_prod_view_create_new.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_create_new.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_cutover_prod_view_create_new.sql
	fi
	
