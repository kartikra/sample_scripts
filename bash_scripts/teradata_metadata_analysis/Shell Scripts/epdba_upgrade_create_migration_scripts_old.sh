#!/usr/bin/ksh

#  Staging Table Count from manifest vs what got created
#  Take Count from all views to check for any failure
#  Difference in Columns between user view, table and  view
#  UPGR_ check if original not being replaced
#  Count of views with lastaltertimestamp and not= UPGR_ must be 0
#  Logic for TPF Store Proc and Mat View Impact


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


	relName="sc0730"
	ticketNo="CRQ000000218034"
    regionProfile="RESC"
	dbChgList="sc0730_chglist_v2.txt"
	stagingList="sc0730_staging_list_v2.txt"

	# relName=$1
	# ticketNo=$2
	# regionProfile=$3
	# dbChgList=$4
	# stagingList=$5
	
	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	#region=`echo $relName | awk '{print substr($0,1,2)}' | tr '[a-z]' '[A-Z]'`

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
	-e 's/'MY_WITS_D_DB'/'$devCalcReportDB4'/g' -e 's/'MY_WITS_E_DB'/'$devCalcReportDB5'/g' -e 's/'MY_WITS_F_DB'/'$devCalcReportDB6'/g' \
	-e 's/'MY_CALC_A_DB'/'$prodCalcReportDB1'/g' -e 's/'MY_CALC_B_DB'/'$prodCalcReportDB2'/g' -e 's/'MY_CALC_C_DB'/'$prodCalcReportDB3'/g' \
	-e 's/'MY_CALC_D_DB'/'$prodCalcReportDB4'/g' -e 's/'MY_CALC_E_DB'/'$prodCalcReportDB5'/g' -e 's/'MY_CALC_F_DB'/'$prodCalcReportDB6'/g' \
	$SQLDIR/accdba_migration_analysis.sql > $TEMPDIR/"$relName"_accdba_migration_analysis.sql
	
	
	# Generate Migration Analysis File
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $TEMPDIR/"$relName"_accdba_migration_analysis.sql $OUTDIR/"$relName"_migration_analysis.out | tee -a  $logFileName

	count1=`cat $DIR/$dbChgList | grep -i "Table Add" | cut -f2 -d'|' | sort | uniq | wc -l | sed 's/\ //g'`
	count2=`cat $DIR/$dbChgList | grep -v -i "Table Add" | grep -v -i "Table Drop" | grep -v -i "Rename Table" | grep -v -i "View Only" | cut -f2 -d'|' | sort | uniq | wc -l | sed 's/\ //g'`

	echo "V1|New Tables|$count1" > $SQLDIR/"$relName"_validation_summary_scripts.sql
	echo "V2|Existing Tables|$count2" >> $SQLDIR/"$relName"_validation_summary_scripts.sql

	echo "Entry has been made in Metadata Table !! " >> $logFileName


#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-4 Creation of Temporary Tables

	#---------------------------------------------------------------------------------------------
	#------------------------ Creation of Temporary Reporting Tables -----------------------------
	#---------------------------------------------------------------------------------------------

	rm -f $SQLDIR/"$relName"_script_generated_exceptions.sql
	rm -f $SQLDIR/"$relName"_tempstructure_for_new_tables.sql
	rm -f $SQLDIR/"$relName"_tempstructure_for_existing_tables.sql
	rm -f $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
	rm -f $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
	rm -f $SQLDIR/"$relName"_validation_of_tempstructure.sql
	rm -f $SQLDIR/"$relName"_w2p_exp_new_rptables_ddl.sql
	rm -f $SQLDIR/"$relName"_w2p_exp_existing_rptables_ddl.sql
	
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line2 ; 
	do      
		
		type=`echo $line2 | cut -f1 -d'|'`
		srcDB=`echo $line2 | cut -f2 -d'|'`
		tempDB=`echo $line2 | cut -f3 -d'|'`
		prodDB=`echo $line2 | cut -f4 -d'|'`
		table=`echo $line2 | cut -f5 -d'|'`
		
		rm -f $TEMPDIR/"$prodDB"_getddl.sql
		rm -f $TEMPDIR/"$srcDB"_getddl.sql

		if [ "$type" == "1" ]
		then

			# New Table Being Added. Just Copy the ddl from WITS and Change the DB Name

			echo "SHOW TABLE $srcDB.\"$table\";" >> $TEMPDIR/"$srcDB"_getddl.sql

			echo "SHOW TABLE $srcDB.\"$table\";" >> $SQLDIR/"$relName"_w2p_exp_new_rptables_ddl.sql


			$SCRIPTDIR/epdba_runSQLFile.sh "$TDDEV" "$TEMPDIR/"$srcDB"_getddl.sql" $TEMPDIR/"$table"_witsddl.out | tee -a  $logFileName
            sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//'  -e 's/'$srcDB'/'$tempDB'/g'  $TEMPDIR/"$table"_witsddl.out >  $TEMPDIR/"$table"_prodddl.out
			cat $TEMPDIR/"$table"_prodddl.out >> $SQLDIR/"$relName"_tempstructure_for_new_tables.sql 

		else

			echo "SHOW TABLE $srcDB.\"$table\";" >> $SQLDIR/"$relName"_w2p_exp_existing_rptables_ddl.sql

			echo "SHOW TABLE $srcDB.\"$table\";" >> $TEMPDIR/"$srcDB"_getddl.sql
			echo "SHOW TABLE $prodDB.\"$table\";" >> $TEMPDIR/"$prodDB"_getddl.sql

			rm -f $TEMPDIR/"$table"_witsddl.out
			rm -f $TEMPDIR/"$table"_prodddl.out
			rm -f $TEMPDIR/"$table"_manifest_changes.out
		
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$srcDB"_getddl.sql $TEMPDIR/"$table"_witsddl.out | tee -a  $logFileName
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$prodDB"_getddl.sql $TEMPDIR/"$table"_prodddl.out | tee -a  $logFileName
			cat $DIR/$dbChgList | grep -w -i $table > $TEMPDIR/"$table"_manifest_changes.out | tee -a  $logFileName


			rm -f $TEMPDIR/"$table"_newcols_prodddl.out

			if [ ! -s $TEMPDIR/"$table"_witsddl.out ]
			then
				echo "WITS DEFINITON ERROR : Table $srcDB.$table Not Found in $TDDEV" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi
			if [ ! -s $TEMPDIR/"$table"_prodddl.out ]
			then
				echo "PROD DEFINITON ERROR : Table $prodDB.$table Not Found in $TDPROD" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi

			if [ -s $TEMPDIR/"$table"_prodddl.out ] && [ -s $TEMPDIR/"$table"_witsddl.out ]
			then
			
				cat "$TEMPDIR/"$table"_manifest_changes.out"  |  while read -r line ; do

					type=`echo $line | cut -f1 -d'|'`
					table=`echo $line | cut -f2 -d'|'`
					column=`echo $line | cut -f3 -d'|'`
					pdef=`echo $line | cut -f4 -d'|'`
					wdef=`echo $line | cut -f5 -d'|'`
					#srcDB=`echo $line | cut -f7 -d'|'`
					#tempDB=`echo $line | cut -f8 -d'|'`
					#prodDB=`echo $line | cut -f9 -d'|'`


					# Get the column defitnion from WITS. Exclude the line where column is defined as part of PI
					colToReplace=`cat $TEMPDIR/"$table"_witsddl.out | grep -w -i $column | grep -v 'CONSTRAINT ' | grep -v 'UNIQUE PRIMARY INDEX'  | grep -v '\-\-'`


					# Get Line Number from first match. If same column is also PI, then we can get more than 1 match

					grep -w -n "$column" $TEMPDIR/"$table"_witsddl.out > $TEMPDIR/get_line_no.dat
					witsLine=`head -1 $TEMPDIR/get_line_no.dat | cut -f1 -d':'` 
					witsLineText=`head -1 $TEMPDIR/get_line_no.dat | cut -f2 -d':'`
					colNameSameAsTableInd=`echo $witsLineText | grep -i "$srcDB" | wc -l`
					if [ $colNameSameAsTableInd -eq 1 ]
					then
						witsLine=`head -2 $TEMPDIR/get_line_no.dat | tail -1 | cut -f1 -d':'` 
						witsLineText=`head -2 $TEMPDIR/get_line_no.dat | tail -1 | cut -f2 -d':'`
					fi
					
					if [ -z "$witsLine" ]
					then

						if [ "$prodDB" = "$prodReportDB" ] 
						then
							echo "WITS DEFINITON ERROR : Column $column Not Found in $srcDB.$table " >> $SQLDIR/"$relName"_script_generated_exceptions.sql
						fi
						
					else

						witsLineDef=`echo $witsLineText  | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g'`
						witsLineDef1=`echo $witsLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,1,length-1)}'`
						witsLastChar=`echo $witsLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,length,1)}'`

						grep -w  -n "$column" $TEMPDIR/"$table"_prodddl.out > $TEMPDIR/get_line_no.dat
						prodLine=`head -1 $TEMPDIR/get_line_no.dat | cut -f1 -d':'`
						prodLineText=`head -1 $TEMPDIR/get_line_no.dat | cut -f2 -d':'`
						colNameSameAsTableInd=`echo $prodLineText | grep -i "$prodDB" | wc -l`
						if [ $colNameSameAsTableInd -eq 1 ]
						then
							prodLine=`head -2 $TEMPDIR/get_line_no.dat | tail -1 | cut -f1 -d':'` 
							prodLineText=`head -2 $TEMPDIR/get_line_no.dat | tail -1 | cut -f2 -d':'`
						fi

						
						if [ -z "$prodLine" ]
						then
							# Column Needs to be added


							# Find previous line in PROD
							prevprodLine=""
							prevwitsLine=`expr $witsLine - 1`

							while [ -z "$prevprodLine" ] -a [ $prevwitsLine -ne 0 ]  
							do 

								sed ''$prevwitsLine'!d' $TEMPDIR/"$table"_witsddl.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed -e 's/^[ \t]*//' > $TEMPDIR/"$table"_prevwitline.out 
								prevwitfieldName=`cat $TEMPDIR/"$table"_prevwitline.out | cut -f1 -d' '`

								grep -w -n "$prevwitfieldName" $TEMPDIR/"$table"_prodddl.out > $TEMPDIR/get_line_no.dat
								prevprodLine=`head -1 $TEMPDIR/get_line_no.dat | cut -f1 -d':'`
								prevprodLineText=`head -1 $TEMPDIR/get_line_no.dat | cut -f2 -d':'`
								colNameSameAsTableInd=`echo $prevprodLineText | grep -i "$prodDB" | wc -l`
								if [ $colNameSameAsTableInd -eq 1 ]
								then
									prevprodLine=`head -2 $TEMPDIR/get_line_no.dat | tail -1 | cut -f1 -d':'` 
									prevprodLineText=`head -2 $TEMPDIR/get_line_no.dat | tail -1 | cut -f2 -d':'`
								fi
								
								
								prevwitsLine=`expr $prevwitsLine - 1`

							done

							
							# Previous Line has been succesfully located if prevwitsLine is not 0. If 0 then error

							if [ $prevwitsLine -ne 0 ] &&  [  ! -z "$prevprodLine" ]
							then

								fileLength=`cat $TEMPDIR/"$table"_prodddl.out | wc -l`
								headLine=`expr $prevprodLine - 1`
								tailLine=`expr $fileLength - $prevprodLine`
								head -$headLine $TEMPDIR/"$table"_prodddl.out > $TEMPDIR/"$table"_prodddl.tmp
													

								prevprodLineDef=`echo $prevprodLineText  | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g'`
								prevprodLineDef1=`echo $prevprodLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,1,length-1)}'`
								prevprodLastChar=`echo $prevprodLineText | sed 's/^ *//;s/ *$//;s/ */ /'| sed 's/[\t]*$//g' | awk '{print substr($0,length,1)}'`

								if [ "$prevprodLastChar" == ";" ] ||  [ "$prevprodLastChar" == ")" ]
								then
									echo "$prevprodLineDef1," >>  $TEMPDIR/"$table"_prodddl.tmp  # Make sure last character ends in a ','
								else
									echo "$prevprodLineDef" >>  $TEMPDIR/"$table"_prodddl.tmp
								fi



								if [ "$witsLastChar" == ";" ] || [ "$witsLastChar" == "," ] || [ "$witsLastChar" == ")" ] 
								then
									echo "/*$relName Column Added*/ $witsLineDef1$witsLastChar" >>  $TEMPDIR/"$table"_prodddl.tmp
								else
									echo "/*$relName Column Added*/ $witsLineDef"  >>  $TEMPDIR/"$table"_prodddl.tmp
								fi


								tail -$tailLine $TEMPDIR/"$table"_prodddl.out >> $TEMPDIR/"$table"_prodddl.tmp

								mv $TEMPDIR/"$table"_prodddl.tmp $TEMPDIR/"$table"_prodddl.out

							else

								"WITS DEFINITON ERROR : Column Add Error - Unable to locate position of $column in $srcDB.$table " >> $SQLDIR/"$relName"_prod_exceptions_temptables_ddl.sql

							fi

							
							#----------------------  Old Logic for COLUMN ADD   ------------------------------------
							#fieldName=`echo $colToReplace | cut -f1 -d' '`
							#fieldType=`echo $colToReplace | cut -f2 -d' '`

							#fieldType=`echo $fieldType | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | sed 's/))/)/g'`
							#lastChar=`echo $fieldType  | awk '{print substr($0,length,1)}'`

							#echo "," >> $TEMPDIR/"$table"_newcols_prodddl.out
							#if [ "$witsLastChar" == ";" ] || [ "$witsLastChar" == "," ] || [ "$witsLastChar" == ")" ]
							#then
							
							#	echo "ADD $witsLineDef1" >> $TEMPDIR/"$table"_newcols_prodddl.out
							#else
							#	echo "ADD $witsLineDef" >> $TEMPDIR/"$table"_newcols_prodddl.out						fi

							#fi
							#-------------------------------------------------------------------------------------


						else

							# Column Needs to be replaced
							# Replace content of $prodLine


							fileLength=`cat $TEMPDIR/"$table"_prodddl.out | wc -l`
							headLine=`expr $prodLine - 1`
							tailLine=`expr $fileLength - $prodLine`
							head -$headLine $TEMPDIR/"$table"_prodddl.out > $TEMPDIR/"$table"_prodddl.tmp
							

							prodLastChar=`echo $prodLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,length,1)}'`

							if [ "$prodLastChar" == ";" ] || [ "$prodLastChar" == "," ] || [ "$prodLastChar" == ")" ] 
							then
								echo "/*$relName Tab Change*/ $witsLineDef1$prodLastChar" >>  $TEMPDIR/"$table"_prodddl.tmp
							else
								echo "/*$relName Tab Change*/ $witsLineDef"  >>  $TEMPDIR/"$table"_prodddl.tmp
							fi

							tail -$tailLine $TEMPDIR/"$table"_prodddl.out >> $TEMPDIR/"$table"_prodddl.tmp
							mv $TEMPDIR/"$table"_prodddl.tmp $TEMPDIR/"$table"_prodddl.out
								

						fi


					fi


				done


				#----------------------  Old Logic for COLUMN ADD   ------------------------------------
				#if [ -s "$TEMPDIR/"$table"_newcols_prodddl.out" ]
				#then
				#	echo "ALTER TABLE $prodDB.\"$table\"" >>  $TEMPDIR/"$table"_prodddl.out
				#	sed '1d' $TEMPDIR/"$table"_newcols_prodddl.out >>  $TEMPDIR/"$table"_prodddl.out
				#	echo ";" >>  $TEMPDIR/"$table"_prodddl.out
				#	echo "" >>  $TEMPDIR/"$table"_prodddl.out
				#fi
				#-------------------------------------------------------------------------------------



				# Index Related Mainuplation

				cat $TEMPDIR/"$table"_prodddl.out | grep  -i -w -n 'CONSTRAINT' > $TEMPDIR/find_index_type1.dat
				if [ -f $TEMPDIR/find_index_type1.dat ]
				then
					if [ -s $TEMPDIR/find_index_type1.dat ]
					then
						idxLineNo=`head -1 $TEMPDIR/find_index_type1.dat | cut -f1 -d':'`
						idxLineText=`head -1 $TEMPDIR/find_index_type1.dat | cut -f2 -d':'`

						prevLineNo=`expr $idxLineNo - 1`

						sed ''$prevLineNo'!d' $TEMPDIR/"$table"_prodddl.out | sed -e 's/^[ \t]*//' > $TEMPDIR/"$table"_prevLine.out 


						fileLength=`cat $TEMPDIR/"$table"_prodddl.out | wc -l`
						headLine=`expr $idxLineNo - 2`
						tailLine=`expr $fileLength - $idxLineNo + 1`
						head -$headLine $TEMPDIR/"$table"_prodddl.out > $TEMPDIR/"$table"_prodddl.tmp


						idxLineDef=`head -1 $TEMPDIR/"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//'`
						idxLineDef1=`head -1 $TEMPDIR/"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,1,length-1)}'`
						idxLineDefLastChar=`head -1 $TEMPDIR/"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,length,1)}'`
						if [ "$idxLineDefLastChar" != "," ]
						then
							echo "$idxLineDef1," >>  $TEMPDIR/"$table"_prodddl.tmp  # Make sure last character ends in a ','
						else
							echo "$idxLineDef" >>  $TEMPDIR/"$table"_prodddl.tmp
						fi

						tail -$tailLine $TEMPDIR/"$table"_prodddl.out >> $TEMPDIR/"$table"_prodddl.tmp
						mv $TEMPDIR/"$table"_prodddl.tmp $TEMPDIR/"$table"_prodddl.out


					fi
				fi


				cat $TEMPDIR/"$table"_prodddl.out | grep -i -w -n 'UNIQUE PRIMARY INDEX' > $TEMPDIR/find_index_type1.dat
				if [ -f $TEMPDIR/find_index_type1.dat ]
				then
					if [ -s $TEMPDIR/find_index_type1.dat ]
					then
						idxLineNo=`head -1 $TEMPDIR/find_index_type1.dat | cut -f1 -d':'`
						idxLineText=`head -1 $TEMPDIR/find_index_type1.dat | cut -f2 -d':'`

						prevLineNo=`expr $idxLineNo - 1`

						sed ''$prevLineNo'!d' $TEMPDIR/"$table"_prodddl.out | sed -e 's/^[ \t]*//' > $TEMPDIR/"$table"_prevLine.out 


						fileLength=`cat $TEMPDIR/"$table"_prodddl.out | wc -l`
						headLine=`expr $idxLineNo - 2`
						tailLine=`expr $fileLength - $idxLineNo + 1`
						head -$headLine $TEMPDIR/"$table"_prodddl.out > $TEMPDIR/"$table"_prodddl.tmp


						idxLineDef=`head -1 $TEMPDIR/"$table"_prevLine.out  | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//'`
						idxLineDef1=`head -1 $TEMPDIR/"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,1,length-1)}'`
						idxLineDefLastChar=`head -1 $TEMPDIR/"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,length,1)}'`

						if [ "$idxLineDefLastChar" != ")" ]
						then
							echo "$idxLineDef1)" >>  $TEMPDIR/"$table"_prodddl.tmp  # Make sure last character ends in a ')'
						else
							echo "$idxLineDef" >>  $TEMPDIR/"$table"_prodddl.tmp
						fi

						tail -$tailLine $TEMPDIR/"$table"_prodddl.out >> $TEMPDIR/"$table"_prodddl.tmp
						mv $TEMPDIR/"$table"_prodddl.tmp $TEMPDIR/"$table"_prodddl.out


					fi
				fi

				sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$tempDB'/g' $TEMPDIR/"$table"_prodddl.out >> $SQLDIR/"$relName"_tempstructure_for_existing_tables.sql 

				
				echo "EXEC CLARITY_DBA_MAINT.CLARITY_UPG_TABLE_COMPARISON ('$tempDB','$table', '$prodDB','$table');" >> $SQLDIR/"$relName"_validation_of_tempstructure.sql
			fi
		
		fi


		#-----------------------------------------------------------------------------------#
		#--------------------------------  Logic for TPF Tables ----------------------------#
		#------------------- Generate the Script for creating TPF Tables -------------------#
		#-----------------------------------------------------------------------------------#


		#------ Export the DDL for TPF Table if Found  -----
			cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | cut -f2,3 -d'|' | sort | uniq | while read -r line ; do

				tpfDB=`echo $line | cut -f1 -d'|'`
				tpfTable=`echo $line | cut -f2 -d'|'`


				if [ "$tpfDB" == "$prodDB" ] 
				then
					tpfDB=`echo $tpfDB | sed -e 's/_/_TPF_/'`
				fi

				newTabName=$tpfDB.U_"$prefix"_"$table"

				echo "RENAME TABLE /*$ticketNo*/ $tpfDB.\"$table\" TO $newTabName;" >> $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
				sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$tpfDB'/g' -e 's/'$tempDB'/'$tpfDB'/g' $TEMPDIR/"$table"_prodddl.out >> $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
			done
		

		
		#-----------------------------------------------------------------------------------#
		#------------------  Logic for Materialized View Tables ----------------------------#
		#-------------- Generate the Script for creating Mat View Tables -------------------#
		#-----------------------------------------------------------------------------------#

		#------ Export the DDL for MatView Table if Found  -----
			cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND MAT VIEW TABLE ENTRY" | grep -i -w $table | cut -f5,6,7 -d'|' | sort | uniq | while read -r line ; do
				
				reportTable=`echo $line | cut -f1 -d'|'`
				matViewDB=`echo $line | cut -f2 -d'|'`
				matViewTable=`echo $line | cut -f3 -d'|'`

				newTabName=$matViewDB.U_"$prefix"_"$matViewTable"

				echo "RENAME TABLE /*$ticketNo*/ $matViewDB.\"$matViewTable\" TO $newTabName;" >> $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
				sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$matViewDB'/g' -e 's/'$tempDB'/'$matViewDB'/g' -e 's/'$table'/'$matViewTable'/g'  $TEMPDIR/"$table"_prodddl.out >> $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
			done		
		
		

		rm -f $TEMPDIR/"$table"_manifest_changes.out
		rm -f $TEMPDIR/"$table"_newcols_prodddl.out
		rm -f $TEMPDIR/"$table"_witsddl.out
		rm -f $TEMPDIR/"$table"_prodddl.out

		rm -f $TEMPDIR/"$table"_*.*

	done

	$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_tempstructure_for_new_tables.sql N
	$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_tempstructure_for_existing_tables.sql N

	if [ -f $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_tpf_new_structure_ddl.sql N
	fi


#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-5a Get  View DDL from WITS. Convert it to Prod Definition

echo "---------------------------------------------------------------" >> $logFileName
echo "Copying  DDL for  Views from WITS to PROD ... " >> $logFileName


	#---------------------------------------------------------------------------------------------------
	#------------------------- Creation of  Views for Reporting Tables -----------------------------
	#---------------------------------------------------------------------------------------------------

	rm -f $SQLDIR/"$relName"_cutover_prod_view_*.sql
	rm -f $SQLDIR/"$relName"_cutover_tpf_view.sql

	#$SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql
	#$SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql
	#$SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql
	#$SQLDIR/"$relName"_cutover_prod_view_create_new.sql


	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND VIEW ENTRY" | cut -f2,3 -d'|' | sort | uniq | while read -r line2 ; do

		table=`echo $line2 | cut -f1 -d'|'`
		tpfInd=`echo $line2 | cut -f2 -d'|'`


		echo "SHOW VIEW $devView.\"$table\";" > $TEMPDIR/"$TDDEV"_view.sql
		echo "SHOW VIEW $prodView.\"$table\";" > $TEMPDIR/"$TDPROD"_view.sql

		rm -f $TEMPDIR/"$TDDEV"_"$table"_view.out
		rm -f $TEMPDIR/"$TDPROD"_"$table"_view.out

		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$TDDEV"_view.sql $TEMPDIR/"$TDDEV"_"$table"_view.out | tee -a  $logFileName
		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$TDPROD"_view.sql $TEMPDIR/"$TDPROD"_"$table"_view.out | tee -a  $logFileName
		
		cat $DIR/$dbChgList | grep -w -i $table > $TEMPDIR/"$table"_manifest_changes.out | tee -a  $logFileName

		if [ -s $TEMPDIR/"$TDDEV"_"$table"_view.out ]
		then
			#  View exist in WITS. Copy to Prod

			sed -e 's/'$devReportDB'/'$prodReportDB'/g'  -e 's/'$devView'/'$prodView'/g' \
				-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
				-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
				-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
			$TEMPDIR/"$TDDEV"_"$table"_view.out  >> $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql

			
			#--------------------------------  Logic for TPF Tables ----------------------------#
                       	# Add  View For Existing TPF Tables

			sed -e 's/'$devReportDB'/'$prodTpfReportDB'/g'  -e 's/'$devView'/'$prodTpfView'/g' \
				-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
				-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
				-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
			$TEMPDIR/"$TDDEV"_"$table"_view.out  > $TEMPDIR/view_tpftable.out


		else
		
			# Not Found in WITS. Refresh from Production
			
			if [ -s $TEMPDIR/"$TDPROD"_"$table"_view.out ]
			then
				dataResInd=`grep -w -n "ROLE IN" $TEMPDIR/"$TDPROD"_"$table"_view.out | wc -l`
		
				# Refresh  View in Production
		
				if [ $dataResInd -ne 0 ]
				then

					#  View Exisits in Production with Data Restrictions
					# Add the new columns to the view definition

					grep -w -n "FROM" $TEMPDIR/"$TDPROD"_"$table"_view.out | grep -v '\-\-' | cut -f1 -d':' > $TEMPDIR/"$TDPROD"_"$table"_prodLine.out
					fileLength=`cat $TEMPDIR/"$TDPROD"_"$table"_view.out | wc -l`


					head -1 $TEMPDIR/"$TDPROD"_"$table"_prodLine.out | sort | uniq | while read -r line2 ; do

						headLine=`expr $line2 - 1`
						tailLine=`expr $fileLength - $line2 + 1`
						head -$headLine $TEMPDIR/"$TDPROD"_"$table"_view.out > $TEMPDIR/"$TDPROD"_"$table"_view.tmp

						cat $DIR/$dbChgList | grep -w -i $table | cut -f3 -d'|'  > $TEMPDIR/getcols.tmp

						headerAdded="F"
						cat $TEMPDIR/getcols.tmp | while read -r fieldName; do

							foundInd=`cat $TEMPDIR/"$TDPROD"_"$table"_view.out | grep -i -w "$fieldName" | wc -l`
							if [ $foundInd -eq 0 ] && [ "$headerAdded" == "F" ]
							then
								echo "/* Addition of Columns in WITS */" >> $TEMPDIR/"$TDPROD"_"$table"_view.tmp
								headerAdded="T"
							fi
							
							if [ $foundInd -eq 0 ]
							then
								echo ",\"$fieldName\""  >> $TEMPDIR/"$TDPROD"_"$table"_view.tmp
							fi

						done


						tail -$tailLine $TEMPDIR/"$TDPROD"_"$table"_view.out >> $TEMPDIR/"$TDPROD"_"$table"_view.tmp
						mv $TEMPDIR/"$TDPROD"_"$table"_view.tmp $TEMPDIR/"$TDPROD"_"$table"_view.out

					done

					sed -e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
					$TEMPDIR/"$TDPROD"_"$table"_view.out >> $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql


				else

					#  View Exisits in Production without Data Restrictions
					# Refresh current view in production

					sed -e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
					$TEMPDIR/"$TDPROD"_"$table"_view.out >> $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql


				fi
				
				#--------------------------------  Logic for TPF Tables ----------------------------#
                		# Add  View For Existing TPF Tables
				

				sed -e 's/'$devReportDB'/'$prodTpfReportDB'/g'  -e 's/'$devView'/'$prodTpfView'/g' \
					-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
					-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
                    -e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
					$TEMPDIR/"$TDPROD"_"$table"_view.out > $TEMPDIR/view_tpftable.out

	 
			else
			
				#  View does not exist in WITS and Prod !!
				# Create a new view as SELECT * FROM <Reporting Table>

				sed -e 's/'MY_TGT_DB'/'$prodView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
				-e 's/'MY_SRC_DB'/'$prodReportDB'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
				>> $SQLDIR/"$relName"_cutover_prod_view_create_new.sql
			
			
				#--------------------------------  Logic for TPF Tables ----------------------------#
                       		# Refresh  View For New TPF Tables

				#sed -e 's/'MY_TGT_DB'/'$prodTpfView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
				#-e 's/'MY_SRC_DB'/'$prodTpfReportDB'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
				#> $TEMPDIR/view_tpftable.out
						
						
			fi
			
		fi

		tpfInd2=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | wc -l`
		if [ $tpfInd2 -ne 0 ]
		then
			cat $TEMPDIR/view_tpftable.out >> $SQLDIR/"$relName"_cutover_tpf_view.sql
		fi


		rm -f $TEMPDIR/"$TDPROD"_"$table"_prodLine.out
		rm -f $TEMPDIR/"$TDDEV"_"$table"_view.out
		rm -f $TEMPDIR/"$TDPROD"_"$table"_view.out
		rm -f $TEMPDIR/"$table"_manifest_changes.out

	done


	if [ -f $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_refresh_with_dr.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_cutover_prod_view_refresh_with_dr.sql 
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_refresh_without_dr.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_cutover_prod_view_refresh_without_dr.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_copy_from_wits.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_cutover_prod_view_copy_from_wits.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_view_create_new.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_view_create_new.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_cutover_prod_view_create_new.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_tpf_view.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_tpf_view.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_cutover_tpf_view.sql
	fi



#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-5b Get User View DDL from WITS. Convert it to Prod Definition

echo "---------------------------------------------------------------" >> $logFileName
echo "Copying  DDL for USER Views from WITS to PROD ... " >> $logFileName

	#---------------------------------------------------------------------------------------------------
	#------------------------- Creation of User Views for Reporting Tables -----------------------------
	#---------------------------------------------------------------------------------------------------

	rm -f $SQLDIR/"$relName"_cutover_tpf_userview.sql
	rm -f $SQLDIR/"$relName"_cutover_prod_userview_*.sql

	#$SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql
	#$SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql
	#$SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits.sql
	#$SQLDIR/"$relName"_cutover_prod_userview_create_new.sql


	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND VIEW ENTRY" | cut -f2,3 -d'|' | sort | uniq | while read -r line2 ; do

		table=`echo $line2 | cut -f1 -d'|'`
		tpfInd=`echo $line2 | cut -f2 -d'|'`


		echo "SHOW VIEW $devUserView.\"$table\";" > $TEMPDIR/"$TDDEV"_userview.sql
		echo "SHOW VIEW $prodUserView.\"$table\";" > $TEMPDIR/"$TDPROD"_userview.sql

		rm -f $TEMPDIR/"$TDDEV"_"$table"_userview.out
		rm -f $TEMPDIR/"$TDPROD"_"$table"_userview.out

		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$TDDEV"_userview.sql $TEMPDIR/"$TDDEV"_"$table"_userview.out | tee -a  $logFileName
		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$TDPROD"_userview.sql $TEMPDIR/"$TDPROD"_"$table"_userview.out | tee -a  $logFileName
		
		cat $DIR/$dbChgList | grep -w -i $table > $TEMPDIR/"$table"_manifest_changes.out | tee -a  $logFileName

		if [ -s $TEMPDIR/"$TDPROD"_"$table"_userview.out ]
		then
			dataResInd=`grep -w -n "ROLE" $TEMPDIR/"$TDPROD"_"$table"_userview.out | wc -l`
		
			if [ $dataResInd -ne 0 ]
			then

				# User View Exisits in Production with Data Restrictions
				# Add the new columns to the view definition

				grep -w -n "FROM" $TEMPDIR/"$TDPROD"_"$table"_userview.out | grep -v '\-\-' | cut -f1 -d':' > $TEMPDIR/"$TDPROD"_"$table"_prodLine.out
				fileLength=`cat $TEMPDIR/"$TDPROD"_"$table"_userview.out | wc -l`


				head -1 $TEMPDIR/"$TDPROD"_"$table"_prodLine.out | sort | uniq | while read -r line2 ; do

					headLine=`expr $line2 - 1`
					tailLine=`expr $fileLength - $line2 + 1`
					head -$headLine $TEMPDIR/"$TDPROD"_"$table"_userview.out > $TEMPDIR/"$TDPROD"_"$table"_userview.tmp

					cat $DIR/$dbChgList | grep -w -i $table | cut -f3 -d'|'  > $TEMPDIR/getcols.tmp

					headerAdded="F"
					cat $TEMPDIR/getcols.tmp | while read -r fieldName; do
					
						foundInd=`cat $TEMPDIR/"$TDPROD"_"$table"_userview.out | grep -i -w "$fieldName" | wc -l`
						if [ $foundInd -eq 0 ] && [ "$headerAdded" == "F" ]
						then
							echo "/* Addition of Columns in WITS */" >> $TEMPDIR/"$TDPROD"_"$table"_userview.tmp
							headerAdded="T"
						fi
						if [ $foundInd -eq 0 ]
						then
							echo ",\"$fieldName\""  >> $TEMPDIR/"$TDPROD"_"$table"_userview.tmp
						fi
					done


					tail -$tailLine $TEMPDIR/"$TDPROD"_"$table"_userview.out >> $TEMPDIR/"$TDPROD"_"$table"_userview.tmp
					mv $TEMPDIR/"$TDPROD"_"$table"_userview.tmp $TEMPDIR/"$TDPROD"_"$table"_userview.out

				done

				
				sed  -e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
					-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
					-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
				$TEMPDIR/"$TDPROD"_"$table"_userview.out >> $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql


			else


				# Check if User View Exisits in WITS with Data Restrictions

				witsdataResInd=`grep -w -n "ROLE" $TEMPDIR/"$TDDEV"_"$table"_userview.out | wc -l`

				if [ $witsdataResInd -ne 0 ]
				then
					sed -e 's/'$devView'/'$prodView'/g'  -e 's/'$devUserView'/'$prodUserView'/g' \
						-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
 					$TEMPDIR/"$TDDEV"_"$table"_userview.out  >> $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits.sql
				
				else
					# User View Exisits in both WITS and Production without Data Restrictions
					# Refresh current view in production

					sed  -e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
						-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g'  -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
					$TEMPDIR/"$TDPROD"_"$table"_userview.out >> $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql
				fi

			fi


		else
			# User View does not exist in Production


			if [ -s $TEMPDIR/"$TDDEV"_"$table"_userview.out ]
			then
				# User View exist in WITS. Copy to Prod

				sed -e 's/'$devView'/'$prodView'/g'  -e 's/'$devUserView'/'$prodUserView'/g' \
					-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
					-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
					-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
 				$TEMPDIR/"$TDDEV"_"$table"_userview.out  >> $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits.sql

			else
				# User View does not exist in WITS and Prod !!
				# Create a new view as SELECT * FROM <VIEW>

				sed -e 's/'MY_TGT_DB'/'$prodUserView'/g'  -e 's/'MY_TGT_TAB'/'$table'/g' -e 's/'MY_SRC_TAB'/'$table'/g' \
				-e 's/'MY_SRC_DB'/'$prodView'/g' -e 's/'MY_TICKET'/'$ticketNo'/g' $SQLDIR/accdba_create_new_view.sql \
				-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ \ '/'REPLACE\ '/g' \
				>> $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql
				
			fi


		fi


 		#--------------------------------  Logic for TPF Tables ----------------------------#
                # Refresh User View For Exisiting TPF Tables

			sed -e 's/'$prodView'/'$prodTpfView'/g'  -e 's/'$prodUserView'/'$prodTpfUserView'/g' \
			    -e 's/'$devView'/'$prodTpfView'/g'  -e 's/'$devUserView'/'$prodTpfUserView'/g' \
				-e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
				-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
                -e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g'  -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
			$TEMPDIR/"$TDPROD"_"$table"_userview.out > $TEMPDIR/userview_tpftable.out



		tpfInd2=`cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | wc -l`
		if [ $tpfInd2 -ne 0 ]
		then
			cat $TEMPDIR/userview_tpftable.out >> $SQLDIR/"$relName"_cutover_tpf_userview.sql
		fi




		rm -f $TEMPDIR/"$TDPROD"_"$table"_prodLine.out
		rm -f $TEMPDIR/"$TDDEV"_"$table"_userview.out
		rm -f $TEMPDIR/"$TDPROD"_"$table"_userview.out
		rm -f $TEMPDIR/"$table"_manifest_changes.out





	done

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_refresh_with_dr.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_cutover_prod_userview_refresh_with_dr.sql 
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_refresh_without_dr.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_cutover_prod_userview_refresh_without_dr.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_copy_from_wits.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_cutover_prod_userview_copy_from_wits.sql
	fi

	if [ -f $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_prod_userview_create_new.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_cutover_prod_userview_create_new.sql
	fi


	if [ -f $SQLDIR/"$relName"_cutover_tpf_userview.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_tpf_userview.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_cutover_tpf_userview.sql
	fi



echo "View Definition Scripts Created !!" >> $logFileName
echo "---------------------------------------------------------------" >> $logFileName


echo " Reporting Table and View Definitions Created for Migration !! " >> $logFileName
echo "---------------------------------------------------------------" >> $logFileName



#----------------------------------------------------------------------------------------------------------------------------------------#

# STEP-6 Generation of Conversion Scripts. WITS to PROD (Temp Table)

echo "Creating Conversion Scripts ... " >> $logFileName
echo "---------------------------------------------------------------" >> $logFileName


	rm -f $SQLDIR/"$relName"_migrate_tabcol_add.sql 
	rm -f $SQLDIR/"$relName"_migrate_dtype_change_3.sql    # File needs less analysis
	rm -f $SQLDIR/"$relName"_migrate_dtype_change_4.sql    # File needs more analysis
	rm -f $SQLDIR/"$relName"_cutover_tpf_migrate_data.sql
	rm -f $SQLDIR/"$relName"_cutover_matviewdb_migrate_data.sql
	rm -f $SQLDIR/"$relName"_validation_check_for_dataconversion.sql

	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line ; do      
		
		type=`echo $line | cut -f1 -d'|'`
		witsDB=`echo $line | cut -f2 -d'|'`
		tempDB=`echo $line | cut -f3 -d'|'`
		prodDB=`echo $line | cut -f4 -d'|'`
		table=`echo $line | cut -f5 -d'|'`

		echo " Starting for $table ... "  >> $logFileName

		rm -f $TEMPDIR/getcol.sql
		rm -f $TEMPDIR/getcol.out
		rm -f $TEMPDIR/$scriptName-temp0.out
		rm -f $TEMPDIR/$scriptName-temp1.out
		rm -f $TEMPDIR/$scriptName-temp2.out
		rm -f $TEMPDIR/$scriptName-temp3.out
		

		if [ "$type" != "1" ] 		#  Existing Tables
		then

			echo "sel ',' || TRIM(ColumnName) from DBC.Columns WHERE tableName='$table' AND DatabaseName='$prodDB';"  >> $TEMPDIR/getcol.sql
			$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" "$TEMPDIR/getcol.sql" "$TEMPDIR/getcol.out" | tee -a  $logFileName
			sed '1,2d'  $TEMPDIR/getcol.out > $TEMPDIR/$scriptName-temp0.out

			# Add " before and after each fieldName
			cat $TEMPDIR/$scriptName-temp0.out | while read -r fileLine; do
				var2=`echo $fileLine | awk '{print substr($0,2,length)}'`
				echo ",\"$var2\"" >> $TEMPDIR/$scriptName-temp1.out
			done



			if [ -f $TEMPDIR/$scriptName-temp1.out ]
			then

				echo "INSERT INTO /*$ticketNo*/ $tempDB.\"$table\""	>> $TEMPDIR/$scriptName-temp2.out
				echo "(" 						>> $TEMPDIR/$scriptName-temp2.out
				sed '1s/^.//' $TEMPDIR/$scriptName-temp1.out 		>> $TEMPDIR/$scriptName-temp2.out  # Delete the first character from first line (,)			
				echo ") " 						>> $TEMPDIR/$scriptName-temp2.out
				echo "SELECT "					>> $TEMPDIR/$scriptName-temp2.out



				# Handle INT to VARCHAR and DECIMAL to VARCHAR
				cat $DIR/$dbChgList | grep -w -i "$table" | grep -i "Column Datatype difference" |  cut -f3,4,5 -d'|' | sort | uniq | while read -r fieldLine ; do

					fieldName=`echo $fieldLine | cut -f1 -d'|'`
					oldType=`echo $fieldLine | cut -f2 -d'|'`
					newType=`echo $fieldLine | cut -f3 -d'|'`

					flag0=`echo $newType | grep -i "VARCHAR" | wc -l`
					flag1=`echo $oldType | grep -i "INTEGER" | wc -l`
					flag2=`echo $oldType | grep -i "DECIMAL" | wc -l`


					if [ $flag0 -ne 0 ] && [ $flag2 -ne 0 ]
					then
						perl -pi -e  's/"'$fieldName'"/TRIM(TRIM(TRAILING "." FROM "'$fieldName'"))/i'  $TEMPDIR/$scriptName-temp1.out
						mv $TEMPDIR/$scriptName-temp1.out $TEMPDIR/$scriptName-temp50.out
						sed -e 's/'\"\.\"'/'\'\.\''/g' $TEMPDIR/$scriptName-temp50.out > $TEMPDIR/$scriptName-temp1.out
						
						echo "SELECT \"$fieldName\" || '|' AS \"$fieldName\" FROM $tempDB.\"$table\" WHERE \"$fieldName\" IS NOT NULL SAMPLE 5;" >> $SQLDIR/"$relName"_validation_check_for_dataconversion.sql
					fi

					if [ $flag0 -ne 0 ] && [ $flag1 -ne 0 ]
					then
						perl -pi -e  's/"'$fieldName'"/TRIM ("'$fieldName'")/i'  $TEMPDIR/$scriptName-temp1.out
						
						echo "SELECT \"$fieldName\" || '|' AS \"$fieldName\" FROM $tempDB.\"$table\" WHERE \"$fieldName\" IS NOT NULL SAMPLE 5;" >> $SQLDIR/"$relName"_validation_check_for_dataconversion.sql
					fi

				done



				sed '1s/^.//' $TEMPDIR/$scriptName-temp1.out 		>> $TEMPDIR/$scriptName-temp2.out		
				echo " FROM $prodDB.\"$table\";" 			>> $TEMPDIR/$scriptName-temp2.out
				echo " " 						>> $TEMPDIR/$scriptName-temp2.out

			
				rowCounter1=`cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -i "Column Datatype difference"  | wc -l` 		
				if [ $rowCounter1 -ne 0 ]
				then
					echo "/* Columns with Datatype changes " 	>> $TEMPDIR/$scriptName-temp2.out
					cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -i "Column Datatype difference"  | grep -w -i $table | cut -f3,4,5 -d'|' >> $TEMPDIR/$scriptName-temp2.out
					echo "*/ " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
				fi


				rowCounter2=`cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -i 'COLUMN ADD'  | wc -l` 		
				if [ $rowCounter2 -ne 0 ]
				then
					echo "/* Columns added to table " 		>> $TEMPDIR/$scriptName-temp2.out
					cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -i 'COLUMN ADD' | grep -w -i $table | cut -f3 -d'|' >> $TEMPDIR/$scriptName-temp2.out
					echo "*/ " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
				fi


				rowCounter3=`cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -v -i 'TABLE ADD' | grep -v -i 'COLUMN ADD' | grep -v -i 'Column Datatype difference'   | wc -l`
				if [ $rowCounter3 -ne 0 ]
				then
					echo "/* Columns without dataype changes " 	>> $TEMPDIR/$scriptName-temp2.out
					cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -v -i 'TABLE ADD' | grep -v -i 'COLUMN ADD' | grep -v -i 'Column Datatype difference'  | cut -f3,4,5 -d'|' >> $TEMPDIR/$scriptName-temp2.out
					echo "*/ " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
				fi



				if [ "$type" == "2" ]        # Tables with only column additions
				then
					cat $TEMPDIR/$scriptName-temp2.out >> $SQLDIR/"$relName"_migrate_tabcol_add.sql 
				fi

				if [ "$type" == "3" ]        # Tables with simple datatype changes
				then
					cat $TEMPDIR/$scriptName-temp2.out >> $SQLDIR/"$relName"_migrate_dtype_change_3.sql
				fi

				if [ "$type" == "4" ]        # Tables with complex datattype changes
				then
					cat $TEMPDIR/$scriptName-temp2.out >> $SQLDIR/"$relName"_migrate_dtype_change_4.sql
				fi



				#------ Conversion Script for TPF Table if Found  ---------
				cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | cut -f2,3 -d'|' | sort | uniq | while read -r line2 ; do

					tpfDB=`echo $line2 | cut -f1 -d'|'`
					tpfTable=`echo $line2 | cut -f2 -d'|'`
					newTabName=$tpfDB.U_"$prefix"_"$table"

					if [ $tpfDB == $prodDB ]
					then
						tpfDB=`echo $prodDB | sed -e 's/_/_TPF_/'`
					fi

					rm -f $TEMPDIR/$scriptName-temp3.out

					sed -e 's/'$prodDB.\"$table\"'/'$newTabName'/g' $TEMPDIR/$scriptName-temp2.out > $TEMPDIR/$scriptName-temp3.out
					sed -e 's/'$tempDB.\"$table\"'/'$tpfDB.\"$table\"'/g' $TEMPDIR/$scriptName-temp3.out >> $SQLDIR/"$relName"_cutover_tpf_migrate_data.sql

				done
				
				
				#------ Conversion Script for MatView Table if Found  ---------
				cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND MAT VIEW TABLE ENTRY"  | grep -i -w $table | cut -f5,6,7 -d'|' | sort | uniq | while read -r line2 ; do

					reportTable=`echo $line2 | cut -f1 -d'|'`
					matViewDB=`echo $line2 | cut -f2 -d'|'`
					matViewTable=`echo $line2 | cut -f3 -d'|'`
					newTabName=$matViewDB.U_"$prefix"_"$matViewTable"

					rm -f $TEMPDIR/$scriptName-temp3.out

					sed -e 's/'$prodDB.\"$table\"'/'$newTabName'/g' $TEMPDIR/$scriptName-temp2.out > $TEMPDIR/$scriptName-temp3.out
					sed -e 's/'$tempDB'/'$matViewDB'/g' -e 's/'$table'/'$matViewTable'/g' $TEMPDIR/$scriptName-temp3.out >> $SQLDIR/"$relName"_cutover_matviewdb_migrate_data.sql
					
				done

			else
				echo " $table Not Found in $prodDB" > $SQLDIR/"$relName"_script_generated_exceptions.sql
			
			fi
		fi

	
	done


	echo "Conversion Scripts Created !!" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName





#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-7 Create Stats Collection Scripts. Collecting Stats on Temp Database in PROD


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


		echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (1,'$prodDB','$table','$tempDB',line1);" >> $TEMPDIR/getstats.sql
		
		if [ "$type" != "1" ] 
		then
			# Stats on Existing Tables -- Collect all Existing Stats in PROD
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (1,'$prodDB','$table','$tempDB',line1);" >> $TEMPDIR/get_existing_stats.sql
			
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (1,'$prodDB','$table','$prodDB',line1);" >> $TEMPDIR/create_prod_exisiting_stats.sql

		else
			# Stats on New Tables -- Collect Stats on Primary Index
			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (2,'$tempDB','$table','$tempDB',line1);" >> $TEMPDIR/get_new_stats.sql

			echo "CALL CLARITY_DBA_MAINT.CLARITY_UPG_CREATE_STATS (2,'$tempDB','$table','$prodDB',line1);" >> $TEMPDIR/create_prod_new_stats.sql

		fi

	done
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/get_existing_stats.sql $TEMPDIR/get_existing_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g' $TEMPDIR/get_existing_stats.out > $SQLDIR/"$relName"_migrate_stats_exist.sql
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/get_new_stats.sql $TEMPDIR/get_new_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g'  $TEMPDIR/get_new_stats.out > $SQLDIR/"$relName"_migrate_stats_new.sql
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/create_prod_exisiting_stats.sql $TEMPDIR/create_prod_exisiting_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g'  $TEMPDIR/create_prod_exisiting_stats.out > $SQLDIR/"$relName"_post_cutover_collect_stats_existing_tables.sql
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/create_prod_new_stats.sql $TEMPDIR/create_prod_new_stats.out | tee -a  $logFileName
	sed 's/'CollectStatsSQL'/'\-\-CollectStatsSQL'/g'  $TEMPDIR/create_prod_new_stats.out > $SQLDIR/"$relName"_post_cutover_collect_stats_new_tables.sql
	
	echo "Stats Collection Scripts Created !!" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName



#----------------------------------------------------------------------------------------------------------------------------------------#

# STEP-8 Generation of Cutover Scripts.Rename Current Prod and Copy from TEMP to PROD.


echo "Creating Cutover Scripts ... " >> $logFileName
echo "---------------------------------------------------------------" >> $logFileName

	rm -f $SQLDIR/"$relName"_cutover_prod_rename.sql
	rm -f $SQLDIR/"$relName"_cutover_temp_to_prod.sql
	rm -f $SQLDIR/"$relName"_post_cutover_drop_temp_reporting_tables.sql


	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line ; do      
		
		type=`echo $line | cut -f1 -d'|'`
		witsDB=`echo $line | cut -f2 -d'|'`
		tempDB=`echo $line | cut -f3 -d'|'`
		prodDB=`echo $line | cut -f4 -d'|'`
		table=`echo $line | cut -f5 -d'|'`

		tableNameLen=`expr length "$table"`
		if [ $tableNameLen -gt 24 ]
		then
			newTableName=`echo $table | awk '{print substr($0,1,23)}'`
			newName=$prodDB.U_"$prefix"_"$newTableName"
		else
			newName=$prodDB.U_"$prefix"_"$table"
		fi


		if [ "$type" != "1" ] 
		then
			echo "RENAME TABLE /*$ticketNo*/  $prodDB.\"$table\" TO $newName ;" >> $SQLDIR/"$relName"_cutover_prod_rename.sql
			echo "ALTER TABLE /*$ticketNo*/ $prodDB.$newName,NO FALLBACK;" >> $SQLDIR/"$relName"_cutover_prod_rename.sql 		
			echo "DROP  TABLE /*$ticketNo*/  $newName;" >> $SQLDIR/"$relName"_post_cutover_drop_temp_reporting_tables.sql
		fi

		renamedTable=`cat $OUTDIR/"$relName"_migration_analysis.out | grep -i "SCRIPT FOUND RENAME TABLE"  | grep -i -w "$prodDB" | grep -i -w "$table" | cut -f4 -d'|'`
		# Do not Create table if target table is being renamed from an existing prod table
		if [ "$renamedTable" != "$table" ]
		then
			echo "CREATE TABLE /*$ticketNo*/  $prodDB.\"$table\" AS $tempDB.\"$table\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_cutover_temp_to_prod.sql
		fi
			
	done

	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND RENAME TABLE" | cut -f1,2,3,4 -d'|' | sort | uniq | while read -r line ; do      
		databaseName=`echo $line | cut -f2 -d'|'`
		table=`echo $line | cut -f3 -d'|'`
		newTable=`echo $line | cut -f4 -d'|'`
		
		if [ ! -z "$databaseName" ]
		then
			echo "RENAME TABLE /*$ticketNo*/  $databaseName.\"$table\" TO $databaseName.\"$newTable\" ;" >> $SQLDIR/"$relName"_cutover_prod_rename.sql 
		fi
	done	
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND DROP TABLE" | cut -f1,2,3 -d'|' | sort | uniq | while read -r line ; do      
		databaseName=`echo $line | cut -f2 -d'|'`
		table=`echo $line | cut -f3 -d'|'`
		
		tableNameLen=`expr length "$table"`
		if [ tableNameLen -gt 24 ]
		then
			newTableName=`echo $table | awk '{print substr($0,1,23)}'`
			newName=$databaseName.U_"$prefix"_"$newTableName"
		else
			newName=$databaseName.U_"$prefix"_"$table"
		fi
		
		if [ ! -z "$databaseName" ]
		then
			echo "RENAME TABLE /*$ticketNo*/ $databaseName.\"$table\" TO $newName ;" >> $SQLDIR/"$relName"_cutover_prod_rename.sql 
			echo "ALTER TABLE /*$ticketNo*/ $databaseName.$newName,NO FALLBACK;" >> $SQLDIR/"$relName"_cutover_prod_rename.sql 
			echo "DROP  TABLE /*$ticketNo*/ $newName;" >> $SQLDIR/"$relName"_post_cutover_drop_temp_reporting_tables.sql
		fi
	done

	
	
	
	echo "Cutover Scripts Created !!" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName


#----------------------------------------------------------------------------------------------------------------------------------------#

# STEP-9 Create the Staging Tables	
echo "Creating Staging Table Scripts ... " >> $logFileName
echo "---------------------------------------------------------------" >> $logFileName


	rm -f $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
	rm -f $SQLDIR/"$relName"_staging_tables_drop_current.sql
	rm -f $SQLDIR/"$relName"_staging_tables_create.sql
	rm -f $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql
	
	sed -e 's/MY_DB/'$prodStgDB'/g' $SQLDIR/accdba_check_secondary_index.sql > $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql
	
	rm -f $TEMPDIR/staging_tables_check_secondary_index.sql
	
	# Skip TokenX Tables while dropping and creating staging tables
	cat $DIR/$stagingList | grep -v -i "tokenx"  | while read -r line ; do

		action=`echo $line | cut -f1 -d'|'`
		tableName=`echo $line | cut -f2 -d'|'`
		deployInd=`echo $line | cut -f3 -d'|'`
		
		if [ "$action" != "DROP" ]
		then
			echo "SHOW TABLE $devStgDB.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			echo "'$tableName'" >> $TEMPDIR/staging_tables_check_secondary_index.sql
		fi
		
		echo "DROP TABLE /*$ticketNo*/ $prodStgDB.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
		
		if [ "$region" == "NC" ] || [ "$region" == "SC" ]
		then
			echo "SHOW TABLE $devDeployStgDB1.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			echo "SHOW TABLE $devDeployStgDB2.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			echo "SHOW TABLE $devDeployStgDB3.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			echo "SHOW TABLE $devDeployStgDB4.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			echo "SHOW TABLE $devDeployStgDB5.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			echo "SHOW TABLE $devDeployStgDB6.\"$tableName\"; " >> $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql
			
			echo "DROP TABLE /*$ticketNo*/ $prodDeployStgDB1.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
			echo "DROP TABLE /*$ticketNo*/ $prodDeployStgDB2.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
			echo "DROP TABLE /*$ticketNo*/ $prodDeployStgDB3.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
			echo "DROP TABLE /*$ticketNo*/ $prodDeployStgDB4.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
			echo "DROP TABLE /*$ticketNo*/ $prodDeployStgDB5.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
			echo "DROP TABLE /*$ticketNo*/ $prodDeployStgDB6.\"$tableName\"; " >> $SQLDIR/"$relName"_staging_tables_drop_current.sql
		fi

	done
	
	echo "(" >> $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql
	paste -sd, $TEMPDIR/staging_tables_check_secondary_index.sql >> $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql
	echo ")" >> $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql

	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_w2p_staging_tables_export_ddl.sql $TEMPDIR/"$relName"_w2p_staging_tables_export_ddl.out | tee -a  $logFileName

	sed -e 's/'$devStgDB'/'$prodStgDB'/g' \
	-e 's/'$devDeployStgDB1'/'$prodDeployStgDB1'/g' -e 's/'$devDeployStgDB2'/'$prodDeployStgDB2'/g' -e 's/'$devDeployStgDB3'/'$prodDeployStgDB3'/g' \
	-e 's/'$devDeployStgDB4'/'$prodDeployStgDB4'/g' -e 's/'$devDeployStgDB5'/'$prodDeployStgDB5'/g' -e 's/'$devDeployStgDB6'/'$prodDeployStgDB6'/g' \
	$TEMPDIR/"$relName"_w2p_staging_tables_export_ddl.out > $SQLDIR/"$relName"_staging_tables_create.sql

	if [ -f $SQLDIR/"$relName"_staging_tables_create.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_staging_tables_create.sql N
		perl -pi -e 's/\bTABLE\b/\ TABLE\ \/\*'$ticketNo'\*\//i' $SQLDIR/"$relName"_staging_tables_create.sql
	fi
	
	rm -f $TEMPDIR/"$relName"_staging_tables_check_secondary_index.out
	$SCRIPTDIR/epdba_runSQLFile2.sh $TDPROD $SQLDIR/"$relName"_staging_tables_check_secondary_index.sql $TEMPDIR/"$relName"_staging_tables_check_secondary_index.out | tee -a  $logFileName
	tail +2 $TEMPDIR/"$relName"_staging_tables_check_secondary_index.out > $SQLDIR/"$relName"_staging_tables_drop_secondary_index.sql
	
#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-10 Create TPF Stored Procedures	






#----------------------------------------------------------------------------------------------------------------------------------------#

# STEP-11 Changes to Impacted Materialized Views





#----------------------------------------------------------------------------------------------------------------------------------------#

	echo "---------------------------------------------------------------" >> $logFileName
	echo "---------  Migration Scripts Succesfully Created !! -----------" >> $logFileName
	echo "------------ Ended at `date +%Y-%m-%d\ %H:%M:%S` ------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName


