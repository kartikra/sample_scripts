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
# STEP-3 Creation of Temporary Tables
	#---------------------------------------------------------------------------------------------
	#------------------------ Creation of Temporary Reporting Tables -----------------------------
	#---------------------------------------------------------------------------------------------

	rm -f $SQLDIR/"$relName"_tempstructure_for_new_tables.sql
	rm -f $SQLDIR/"$relName"_tempstructure_for_existing_tables.sql
	rm -f $SQLDIR/"$relName"_tempstructure_for_existing_tables_with_PI_change.sql
	rm -f $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
	rm -f $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
	rm -f $SQLDIR/"$relName"_validation_of_tempstructure.sql
	rm -f $SQLDIR/"$relName"_w2p_exp_new_rptables_ddl.sql
	rm -f $SQLDIR/"$relName"_w2p_exp_existing_rptables_ddl.sql
	rm -f $SQLDIR/"$relName"_prod_exp_existing_rptables_ddl.sql
	rm -f $SQLDIR/"$relName"_post_cutover_drop_temp_tpfmat_tables.sql

	
	
	countValue="0"
	
	
	cat $OUTDIR/"$relName"_migration_analysis.out  | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line2 ; 
	do      
		
		type=`echo $line2 | cut -f1 -d'|'`
		srcDB=`echo $line2 | cut -f2 -d'|'`
		tempDB=`echo $line2 | cut -f3 -d'|'`
		prodDB=`echo $line2 | cut -f4 -d'|'`
		table=`echo $line2 | cut -f5 -d'|'`
	
		countValue=`expr $countValue + 1`
	
		if [ "$type" == "1" ]
		then
			cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$srcDB'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
			-e 's/'MY_OUTDDL_FILE'/'"$relName"_"$srcDB"_"$table"\.out'/g' -e 's/'MY_OBJECT'/'TABLE'/g'  >> $SQLDIR/"$relName"_w2p_exp_new_rptables_ddl.sql
			rm -f $TEMPDIR/"$relName"_"$srcDB"_"$table".out
		else
	
			cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$srcDB'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
			-e 's/'MY_OUTDDL_FILE'/'"$relName"_"$srcDB"_"$table"\.out'/g' -e 's/'MY_OBJECT'/'TABLE'/g'  >> $SQLDIR/"$relName"_w2p_exp_existing_rptables_ddl.sql
			rm -f $TEMPDIR/"$relName"_"$srcDB"_"$table".out
			
			cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$prodDB'/g' -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_INDEX'/'$countValue'/g' \
			-e 's/'MY_OUTDDL_FILE'/'"$relName"_"$prodDB"_"$table"\.out'/g' -e 's/'MY_OBJECT'/'TABLE'/g'  >> $SQLDIR/"$relName"_prod_exp_existing_rptables_ddl.sql
			rm -f $TEMPDIR/"$relName"_"$prodDB"_"$table".out
		fi
	
	done
	
	
	# Extract WITS DDL for Reporting Tables
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_w2p_exp_new_rptables_ddl.sql $TEMPDIR/"$scriptName"_witsddl.out | tee -a  $logFileName
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $SQLDIR/"$relName"_w2p_exp_existing_rptables_ddl.sql $TEMPDIR/"$scriptName"_witsddl.out | tee -a  $logFileName

	# Extract PROD DDL for Reporting Tables
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $SQLDIR/"$relName"_prod_exp_existing_rptables_ddl.sql $TEMPDIR/"$scriptName"_prodddl.out | tee -a  $logFileName

	rm -f $SCRIPTDIR/debug2.dat
	rm -f $SCRIPTDIR/debug.dat
	
	# Start Creating scripts for temp tables. Use the DDLs exported above
	
	cat $OUTDIR/"$relName"_migration_analysis.out  | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6 -d'|' | sort | uniq | while read -r line2 ; 
	do      
		
		type=`echo $line2 | cut -f1 -d'|'`
		srcDB=`echo $line2 | cut -f2 -d'|'`
		tempDB=`echo $line2 | cut -f3 -d'|'`
		prodDB=`echo $line2 | cut -f4 -d'|'`
		table=`echo $line2 | cut -f5 -d'|'`


		if [ "$type" == "1" ]
		then

			# New Table Being Added. Just Copy the ddl from WITS and Change the DB Name
			
			rm -f $TEMPDIR/"$scriptName"_"$table"_witsddl.out
			cat $TEMPDIR/"$relName"_"$srcDB"_"$table".out > $TEMPDIR/"$scriptName"_"$table"_witsddl.out 
			
			if [ -s $TEMPDIR/"$scriptName"_"$table"_witsddl.out ]
			then
				sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//'  -e 's/'$srcDB'/'$tempDB'/g' \
				$TEMPDIR/"$scriptName"_"$table"_witsddl.out >> $SQLDIR/"$relName"_tempstructure_for_new_tables.sql 
			else
				echo "WITS DEFINITON ERROR : Table $srcDB.$table Not Found in $TDDEV" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi
			
		else
			
			rm -f $TEMPDIR/"$scriptName"_"$table"_witsddl.out
			cat $TEMPDIR/"$relName"_"$srcDB"_"$table".out > $TEMPDIR/"$scriptName"_"$table"_witsddl.out 
			
			rm -f $TEMPDIR/"$scriptName"_"$table"_prodddl.out
			cat $TEMPDIR/"$relName"_"$prodDB"_"$table".out > $TEMPDIR/"$scriptName"_"$table"_prodddl.out 
			
			rm -f $TEMPDIR/"$scriptName"_"$table"_manifest_changes.out
			rm -f $TEMPDIR/"$scriptName"_"$table"_manifest_changes.tmp
			cat $DIR/$dbChgList | grep -w -i $table | grep -w -v -i "TABLE DROP" | grep -w -v -i "RENAME TABLE" | grep -w -v -i "VIEW ONLY" | grep -w -v -i "PI Change" > $TEMPDIR/"$scriptName"_"$table"_manifest_changes.tmp 

			cat $TEMPDIR/"$scriptName"_"$table"_manifest_changes.tmp >> $SCRIPTDIR/debug2.dat
			
			cat $TEMPDIR/"$scriptName"_"$table"_manifest_changes.tmp | while read -r chgLine ;
			do
				calcInd=`echo $chgLine | cut -f6 -d'|'`
				
				if [ "$prodDB" == "$prodReportDB" ]
				then
					# Lead Reporting Table Changes
					if [ "$calcInd" != "Y" ] 
					then
						echo "$chgLine" >> $TEMPDIR/"$scriptName"_"$table"_manifest_changes.out
					fi
				else
					# Calculated Reporting Table Changes
					if [ "$calcInd" == "Y" ]
					then
						echo "$chgLine" >> $TEMPDIR/"$scriptName"_"$table"_manifest_changes.out
					fi
				fi
			done
			rm -f $TEMPDIR/"$scriptName"_"$table"_manifest_changes.tmp
			cat $TEMPDIR/"$scriptName"_"$table"_manifest_changes.out >> $SCRIPTDIR/debug.dat
			
			
			if [ ! -s $TEMPDIR/"$scriptName"_"$table"_witsddl.out ]
			then
				echo "WITS DEFINITON ERROR : Table $srcDB.$table Not Found in $TDDEV" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi
			if [ ! -s $TEMPDIR/"$scriptName"_"$table"_prodddl.out ]
			then
				echo "PROD DEFINITON ERROR : Table $prodDB.$table Not Found in $TDPROD" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi

			if [ -s $TEMPDIR/"$scriptName"_"$table"_prodddl.out ] && [ -s $TEMPDIR/"$scriptName"_"$table"_witsddl.out ]
			then
			
				cat "$TEMPDIR/"$scriptName"_"$table"_manifest_changes.out"  |  while read -r line ; do

					type=`echo $line | cut -f1 -d'|'`
					table=`echo $line | cut -f2 -d'|'`
					column=`echo $line | cut -f3 -d'|'`
					pdef=`echo $line | cut -f4 -d'|'`
					wdef=`echo $line | cut -f5 -d'|'`



					# Get the column defitnion from WITS. Exclude the line where column is defined as part of PI
					colToReplace=`cat $TEMPDIR/"$scriptName"_"$table"_witsddl.out | grep -w -i $column | grep -v 'CONSTRAINT ' | grep -v 'UNIQUE PRIMARY INDEX'  | grep -v '\-\-'`


					# Get Line Number from first match. If same column is also PI, then we can get more than 1 match

					cat $TEMPDIR/"$scriptName"_"$table"_witsddl.out | grep -w -n $column > $TEMPDIR/"$scriptName"_get_line_no.dat
					witsLine=`head -1 $TEMPDIR/"$scriptName"_get_line_no.dat | cut -f1 -d':'` 
					witsLineText=`head -1 $TEMPDIR/"$scriptName"_get_line_no.dat | cut -f2 -d':'`
					colNameSameAsTableInd=`echo $witsLineText | grep -i "$srcDB" | wc -l`
					if [ $colNameSameAsTableInd -eq 1 ]
					then
						witsLine=`head -2 $TEMPDIR/"$scriptName"_get_line_no.dat | tail -1 | cut -f1 -d':'` 
						witsLineText=`head -2 $TEMPDIR/"$scriptName"_get_line_no.dat | tail -1 | cut -f2 -d':'`
					fi
					
					if [ -z "$witsLine" ]
					then

						if [ "$prodDB" == "$prodReportDB" ] && [ ! -z "$column" ]
						then
							echo "WITS DEFINITON ERROR : Column $column Not Found in $srcDB.$table " >> $SQLDIR/"$relName"_script_generated_exceptions.sql
						fi
						
					else

						witsLineDef=`echo $witsLineText  | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g'`
						witsLineDef1=`echo $witsLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,1,length-1)}'`
						witsLastChar=`echo $witsLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,length,1)}'`

						grep -w  -n "$column" $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_get_line_no.dat
						prodLine=`head -1 $TEMPDIR/"$scriptName"_get_line_no.dat | cut -f1 -d':'`
						prodLineText=`head -1 $TEMPDIR/"$scriptName"_get_line_no.dat | cut -f2 -d':'`
						colNameSameAsTableInd=`echo $prodLineText | grep -i "$prodDB" | wc -l`
						if [ $colNameSameAsTableInd -eq 1 ]
						then
							prodLine=`head -2 $TEMPDIR/"$scriptName"_get_line_no.dat | tail -1 | cut -f1 -d':'` 
							prodLineText=`head -2 $TEMPDIR/"$scriptName"_get_line_no.dat | tail -1 | cut -f2 -d':'`
						fi

						
						if [ -z "$prodLine" ]
						then
							# Column Needs to be added


							# Find previous line in PROD
							prevprodLine=""
							prevwitsLine=`expr $witsLine - 1`

							while [ -z "$prevprodLine" ] -a [ $prevwitsLine -ne 0 ]  
							do 

								sed ''$prevwitsLine'!d' $TEMPDIR/"$scriptName"_"$table"_witsddl.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed -e 's/^[ \t]*//' > $TEMPDIR/"$scriptName"_"$table"_prevwitline.out 
								prevwitfieldName=`cat $TEMPDIR/"$scriptName"_"$table"_prevwitline.out | cut -f1 -d' '`

								grep -w -n "$prevwitfieldName" $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_get_line_no.dat
								prevprodLine=`head -1 $TEMPDIR/"$scriptName"_get_line_no.dat | cut -f1 -d':'`
								prevprodLineText=`head -1 $TEMPDIR/"$scriptName"_get_line_no.dat | cut -f2 -d':'`
								colNameSameAsTableInd=`echo $prevprodLineText | grep -i "$prodDB" | wc -l`
								if [ $colNameSameAsTableInd -eq 1 ]
								then
									prevprodLine=`head -2 $TEMPDIR/"$scriptName"_get_line_no.dat | tail -1 | cut -f1 -d':'` 
									prevprodLineText=`head -2 $TEMPDIR/"$scriptName"_get_line_no.dat | tail -1 | cut -f2 -d':'`
								fi
															
								prevwitsLine=`expr $prevwitsLine - 1`

							done

							
							# Previous Line has been succesfully located if prevwitsLine is not 0. If 0 then error

							if [ $prevwitsLine -ne 0 ] &&  [  ! -z "$prevprodLine" ]
							then

								fileLength=`cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | wc -l`
								headLine=`expr $prevprodLine - 1`
								tailLine=`expr $fileLength - $prevprodLine`
								head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
											
								prevprodLineDef=`echo $prevprodLineText  | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g'`
								prevprodLineDef1=`echo $prevprodLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,1,length-1)}'`
								prevprodLastChar=`echo $prevprodLineText | sed 's/^ *//;s/ *$//;s/ */ /'| sed 's/[\t]*$//g' | awk '{print substr($0,length,1)}'`

								if [ "$prevprodLastChar" == ";" ] ||  [ "$prevprodLastChar" == ")" ]
								then
									echo "$prevprodLineDef1," >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp  # Make sure last character ends in a ','
								else
									echo "$prevprodLineDef" >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
								fi


								if [ "$witsLastChar" == ";" ] || [ "$witsLastChar" == "," ] || [ "$witsLastChar" == ")" ] 
								then
									echo "/*$relName Column Added*/ $witsLineDef1$witsLastChar" >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
								else
									echo "/*$relName Column Added*/ $witsLineDef"  >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
								fi


								tail -$tailLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp

								mv $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp $TEMPDIR/"$scriptName"_"$table"_prodddl.out

							else

								"WITS DEFINITON ERROR : Column Add Error - Unable to locate position of $column in $srcDB.$table " >> $SQLDIR/"$relName"_prod_exceptions_temptables_ddl.sql

							fi


						else

							# Column Needs to be replaced
							# Replace content of $prodLine


							fileLength=`cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | wc -l`
							headLine=`expr $prodLine - 1`
							tailLine=`expr $fileLength - $prodLine`
							head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
							

							prodLastChar=`echo $prodLineText | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[\t]*$//g' | awk '{print substr($0,length,1)}'`

							if [ "$prodLastChar" == ";" ] || [ "$prodLastChar" == "," ] || [ "$prodLastChar" == ")" ] 
							then
								echo "/*$relName Tab Change*/ $witsLineDef1$prodLastChar" >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
							else
								echo "/*$relName Tab Change*/ $witsLineDef"  >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
							fi

							tail -$tailLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
							mv $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp $TEMPDIR/"$scriptName"_"$table"_prodddl.out
								

						fi


					fi


				done


				# Check for PI Changes
				piChangeInd=`cat $DIR/$dbChgList |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -i 'PI Change'  | wc -l`

				if [ $piChangeInd -gt 0 ]
				then
					rm -f $TEMPDIR/wits_PI_line_no.dat
					grep -i -w -n "PRIMARY INDEX" $TEMPDIR/"$scriptName"_"$table"_witsddl.out > $TEMPDIR/wits_PI_line_no.dat
					grep -i -w -n "PRIMARY KEY" $TEMPDIR/"$scriptName"_"$table"_witsddl.out >> $TEMPDIR/wits_PI_line_no.dat
					witsLine=`head -1 $TEMPDIR/wits_PI_line_no.dat | cut -f1 -d':'`
					
					rm -f $TEMPDIR/prod_PI_line_no.dat
					grep -i -w -n "PRIMARY INDEX" $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/prod_PI_line_no.dat
					grep -i -w -n "PRIMARY KEY" $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $TEMPDIR/prod_PI_line_no.dat
					prodLine=`head -1 $TEMPDIR/prod_PI_line_no.dat | cut -f1 -d':'`
					headLine=`expr $prodLine - 1`
					fileLength=`cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | wc -l`
					tailLine=`expr $fileLength - $prodLine`
					
					
					# Move all lines upto PI line
					head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					
					# Comment out current PI Line
					tempLine=`head -$prodLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out | tail -1`
					echo "-- $tempLine" >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					# If ) not found, coment out next line as well
					endInd=`echo $tempLine | grep -i ')' | wc -l`
					if [ $endInd -eq 0 ]
					then
						prodLine1=`expr $prodLine + 1`
						tempLine=`head -$prodLine1 $TEMPDIR/"$scriptName"_"$table"_prodddl.out | tail -1`
						echo "-- $tempLine" >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
						tailLine=`expr $tailLine - 1`
					fi
					
					
								# Add New PI line from WITS
					
					# # Check Previous Lines
					# witsprevLine=`expr $witsLine - 1`
					# witsprevLineLastChar=`head -$witsprevLine $TEMPDIR/"$scriptName"_"$table"_witsddl.out | tail -1 | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,length,1)}'`		
					# prodprevLineLastChar=`head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out | tail -1 | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,length,1)}'`
			
					# if [ "$witsprevLineLastChar" != "$prodprevLineLastChar" ]
					# then
						# tmpprodprevnewLine=`head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out | tail -1 | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,1,length-1)}'`
						# prodprevNewLine="$tmpprodprevnewLine$witsprevLineLastChar"
						
						# echo "$prodprevLine" >> $TEMPDIR/test.dat
						# echo "$prodprevNewLine" >> $TEMPDIR/test.dat
						# echo "$witsprevLine" >> $TEMPDIR/test.dat
						# echo "$witsprevLineLastChar" >> $TEMPDIR/test.dat
						# perl -pi -e 's/'$prodprevLine'/'$prodprevNewLine'/gi'  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					# fi
			
					witstempLine=`head -$witsLine $TEMPDIR/"$scriptName"_"$table"_witsddl.out | tail -1`
					echo "/*$relName PI Change */ $witstempLine" >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					
					# If ) not found, add next line from wits as well
					endInd=`echo $witstempLine | grep -i ')' | wc -l`
					if [ $endInd -eq 0 ]
					then
						witsLine1=`expr $witsLine + 1`
						witstempLine1=`head -$witsLine1 $TEMPDIR/"$scriptName"_"$table"_witsddl.out | tail -1`
						echo "/*$relName  PI Change */ $witstempLine1" >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					fi
					
					
					# Add the rest of definition from PROD
					tail -$tailLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					
					# Check if last line has ;
					lastLineInd=`cat $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp | grep -i ';' | grep -v '/-/-' | wc -l`
					if [ $lastLineInd -eq 0 ]
					then
						echo ";" >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
					fi
					
					mv $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp $TEMPDIR/"$scriptName"_"$table"_prodddl.out
			
				fi
				
								
				# Index Related Mainuplation
				cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | grep  -i -w -n 'CONSTRAINT' > $TEMPDIR/"$scriptName"_find_index_type1.dat
				if [ -f $TEMPDIR/"$scriptName"_find_index_type1.dat ]
				then
					if [ -s $TEMPDIR/"$scriptName"_find_index_type1.dat ]
					then
						idxLineNo=`head -1 $TEMPDIR/"$scriptName"_find_index_type1.dat | cut -f1 -d':'`
						idxLineText=`head -1 $TEMPDIR/"$scriptName"_find_index_type1.dat | cut -f2 -d':'`

						prevLineNo=`expr $idxLineNo - 1`

						sed ''$prevLineNo'!d' $TEMPDIR/"$scriptName"_"$table"_prodddl.out | sed -e 's/^[ \t]*//' > $TEMPDIR/"$scriptName"_"$table"_prevLine.out 


						fileLength=`cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | wc -l`
						headLine=`expr $idxLineNo - 2`
						tailLine=`expr $fileLength - $idxLineNo + 1`
						head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp


						idxLineDef=`head -1 $TEMPDIR/"$scriptName"_"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//'`
						idxLineDef1=`head -1 $TEMPDIR/"$scriptName"_"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,1,length-1)}'`
						idxLineDefLastChar=`head -1 $TEMPDIR/"$scriptName"_"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,length,1)}'`
						if [ "$idxLineDefLastChar" != "," ]
						then
							echo "$idxLineDef1," >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp  # Make sure last character ends in a ','
						else
							echo "$idxLineDef" >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
						fi

						tail -$tailLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
						mv $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp $TEMPDIR/"$scriptName"_"$table"_prodddl.out


					fi
				fi


				cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | grep -i -w -n 'UNIQUE PRIMARY INDEX' > $TEMPDIR/"$scriptName"_find_index_type1.dat
				if [ -f $TEMPDIR/"$scriptName"_find_index_type1.dat ]
				then
					if [ -s $TEMPDIR/"$scriptName"_find_index_type1.dat ]
					then
						idxLineNo=`head -1 $TEMPDIR/"$scriptName"_find_index_type1.dat | cut -f1 -d':'`
						idxLineText=`head -1 $TEMPDIR/"$scriptName"_find_index_type1.dat | cut -f2 -d':'`

						prevLineNo=`expr $idxLineNo - 1`

						sed ''$prevLineNo'!d' $TEMPDIR/"$scriptName"_"$table"_prodddl.out | sed -e 's/^[ \t]*//' > $TEMPDIR/"$scriptName"_"$table"_prevLine.out 


						fileLength=`cat $TEMPDIR/"$scriptName"_"$table"_prodddl.out | wc -l`
						headLine=`expr $idxLineNo - 2`
						tailLine=`expr $fileLength - $idxLineNo + 1`
						head -$headLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out > $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp


						idxLineDef=`head -1 $TEMPDIR/"$scriptName"_"$table"_prevLine.out  | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//'`
						idxLineDef1=`head -1 $TEMPDIR/"$scriptName"_"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,1,length-1)}'`
						idxLineDefLastChar=`head -1 $TEMPDIR/"$scriptName"_"$table"_prevLine.out | sed 's/^ *//;s/ *$//;s/ */ /' | sed 's/[ \t]*$//' | awk '{print substr($0,length,1)}'`

						if [ "$idxLineDefLastChar" != ")" ]
						then
							echo "$idxLineDef1)" >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp  # Make sure last character ends in a ')'
						else
							echo "$idxLineDef" >>  $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
						fi

						tail -$tailLine $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp
						mv $TEMPDIR/"$scriptName"_"$table"_prodddl.tmp $TEMPDIR/"$scriptName"_"$table"_prodddl.out


					fi
				fi
				
				if [ $piChangeInd -gt 0 ]
				then
					sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$tempDB'/g' $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $SQLDIR/"$relName"_tempstructure_for_existing_tables_with_PI_change.sql 	
				else
					sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$tempDB'/g' $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $SQLDIR/"$relName"_tempstructure_for_existing_tables.sql 	
				fi
				
				echo "EXEC CLARITY_DBA_MAINT.CLARITY_UPG_TABLE_COMPARISON ('$tempDB','$table', '$prodDB','$table');" >> $SQLDIR/"$relName"_validation_of_tempstructure.sql
			fi
		
		fi


		#-----------------------------------------------------------------------------------#
		#--------------------------------  Logic for TPF Tables ----------------------------#
		#------------------- Generate the Script for creating TPF Tables -------------------#
		#-----------------------------------------------------------------------------------#


		#------ Export the DDL for TPF Table if Found  -----
			cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TPF ENTRY" | grep -i -w $table | cut -f2,3 -d'|' | sort | uniq | while read -r tpfline ; do

				tpfDB=`echo $tpfline | cut -f1 -d'|'`
				tpfTable=`echo $tpfline | cut -f2 -d'|'`


				if [ "$tpfDB" == "$prodDB" ] 
				then
					tpfDB=`echo $tpfDB | sed -e 's/_/_TPF_/'`
				fi

				newTabName=$tpfDB.U_"$prefix"_"$table"

				echo "RENAME TABLE /*$ticketNo*/ $tpfDB.\"$table\" TO $newTabName;" >> $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
				
				if [ "$type" == "1" ]
				then
					sed -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$srcDB'/'$tpfDB'/g' -e 's/'$tempDB'/'$tpfDB'/g' -e 's/'$table'/\"'$matViewTable'\"/g'  $TEMPDIR/"$scriptName"_"$table"_witsddl.out >> $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
				else
					sed  -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$tpfDB'/g' -e 's/'$tempDB'/'$tpfDB'/g' $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql
				fi
				
				echo "DROP  TABLE /*$ticketNo*/  $newTabName;" >> $SQLDIR/"$relName"_post_cutover_drop_temp_tpfmat_tables.sql
			done
		

		#-----------------------------------------------------------------------------------#
		#------------------  Logic for Materialized View Tables ----------------------------#
		#-------------- Generate the Script for creating Mat View Tables -------------------#
		#-----------------------------------------------------------------------------------#

		#------ Export the DDL for MatView Table if Found  -----
			cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND MAT VIEW TABLE ENTRY" | grep -i -w $table | cut -f5,6,7 -d'|' | sort | uniq | while read -r matline ; do
				
				reportTable=`echo $matline | cut -f1 -d'|'`
				matViewDB=`echo $matline | cut -f2 -d'|'`
				matViewTable=`echo $matline | cut -f3 -d'|'`

				newTabName=$matViewDB.U_"$prefix"_"$matViewTable"

				echo "RENAME TABLE /*$ticketNo*/ $matViewDB.\"$matViewTable\" TO $newTabName;" >> $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
				
				if [ "$type" == "1" ]
				then
					sed -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$srcDB'/'$matViewDB'/g' -e 's/'$tempDB'/'$matViewDB'/g' -e 's/'$table'/\"'$matViewTable'\"/g'  $TEMPDIR/"$scriptName"_"$table"_witsddl.out >> $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
				else
					sed -e 's/[Tt][Aa][Bb][Ll][Ee]\ /TABLE\ \/\*'$ticketNo'\*\//' -e 's/'$prodDB'/'$matViewDB'/g' -e 's/'$tempDB'/'$matViewDB'/g' -e 's/'$table'/\"'$matViewTable'\"/g'  $TEMPDIR/"$scriptName"_"$table"_prodddl.out >> $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql
				fi
				echo "DROP  TABLE /*$ticketNo*/  $newTabName;" >> $SQLDIR/"$relName"_post_cutover_drop_temp_tpfmat_tables.sql
			done		
		
		

		rm -f $TEMPDIR/"$scriptName"_"$table"_manifest_changes.out
		rm -f $TEMPDIR/"$scriptName"_"$table"_witsddl.out
		rm -f $TEMPDIR/"$scriptName"_"$table"_prodddl.out

		rm -f $TEMPDIR/"$scriptName"_"$table"_*.*

	done

	
	if [ -f $SQLDIR/"$relName"_tempstructure_for_new_tables.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_tempstructure_for_new_tables.sql N
	fi
	if [ -f $SQLDIR/"$relName"_tempstructure_for_existing_tables.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_tempstructure_for_existing_tables.sql N
	fi
	if [ -f $SQLDIR/"$relName"_cutover_tpf_new_structure_ddl.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_tpf_new_structure_ddl.sql N
	fi
	if [ -f $SQLDIR/"$relName"_cutover_matviewdb_new_structure_ddl.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_cutover_matviewdb_new_structure_ddl.sql N
	fi

	