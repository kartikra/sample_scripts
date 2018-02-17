#!/usr/bin/ksh

#--------------------------------------------------------------------------------
# 1. Order the Columns in the right order
# 2. Add any missing columns from PROD/WITS  view
# 3. Compare with another region
# 4. Apply new restrictions from a input file
#--------------------------------------------------------------------------------


# STEP-1 Read Input Parameters

 	while getopts s:t:v:o:w: par
        do      case "$par" in
                s)    srcProfile="$OPTARG";;
                t)    tgtProfile="$OPTARG";;
				v)    viewList="$OPTARG";;
				o)    option="$OPTARG";;
				w)    ticketNo="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh -s <sourceProfile> -t <tgtProfile> -v <viewList> -o <option> -w <ticketNo> "
                        exit 998;;
                esac
        done


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
				srcView=$devView
				srcUserView=$devUserView
				srcType="WITS"
			else
				if [ ! -z "$TDPROD" ]
				then
					TDSRC="$TDPROD"
					TDPROD=""
					srcStgDB=$prodStgDB
					srcRptDB=$prodReportDB
					srcView=$prodView
					srcUserView=$prodUserView
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
				TDDEV=""
				tgtStgDB=$devStgDB
				tgtRptDB=$devReportDB
				tgtView=$devView
				tgtUserView=$devUserView
				tgtType="WITS"
			else
				if [ ! -z "$TDPROD" ]
				then
					TDTGT="$TDPROD"
					TDPROD=""
					tgtStgDB=$devStgDB
					tgtRptDB=$devReportDB
					tgtView=$devView
					tgtUserView=$devUserView
					tgtType="PROD"
				fi
			fi
        fi
	fi
	
	

# STEP-4 Get the list of columns from target  view and current source defintion 

	rm -f $TEMPDIR/"$scriptName"_column_list.sql
	rm -f $TEMPDIR/"$scriptName"_source_userview.sql
	rm -f $TEMPDIR/column_list_*.out
	rm -f $TEMPDIR/source_userview_*.out
	
	cat $viewList | cut -f1 -d'|' | sort | uniq | while read -r inputTable ; do
	
		echo ".EXPORT RESET;" >> $TEMPDIR/"$scriptName"_column_list.sql
		echo ".EXPORT REPORT FILE = $TEMPDIR/column_list_"$inputTable".out;" >> $TEMPDIR/"$scriptName"_column_list.sql
	
		echo ".EXPORT RESET;" >> $TEMPDIR/"$scriptName"_userview.sql
		echo ".EXPORT REPORT FILE = $TEMPDIR/source_userview_"$inputTable".out;" >> $TEMPDIR/"$scriptName"_userview.sql
	
		if [ ! -z "$TDSRC" ]
		then
			echo "SHOW VIEW $srcUserView.$inputTable;" >> $TEMPDIR/"$scriptName"_source_userview.sql
		else
			echo "SHOW VIEW $tgtUserView.$inputTable;" >> $TEMPDIR/"$scriptName"_source_userview.sql
		fi
		echo "SELECT TRIM(ColumnName) AS DBC_COLUMN FROM DBC.ColumnsV WHERE TRIM(DatabaseName)='$srcView' AND TRIM(TableName)='$inputTable' ORDER BY ColumnId;" >> $TEMPDIR/"$scriptName"_column_list.sql

	done
	
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$scriptName"_source_userview.sql $TEMPDIR/"$scriptName"_source_userview.out | tee -a  $logFileName
	
	$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$scriptName"_column_list.sql $TEMPDIR/"$scriptName"_column_list.out | tee -a  $logFileName



	
# STEP-5 Create the custom view scripts 
	
	rm -f $OUTDIR/custom_order.sql
	rm -f $OUTDIR/custom_order_execptions.out
	rm -f $OUTDIR/custom_order_error.sql
	# rm -f $OUTDIR/custom_order_validation1.sql
	# rm -f $OUTDIR/custom_order_validation2.sql
	
	
	cat $viewList | cut -f1 -d'|' | sort | uniq | while read -r inputTable ; do
	
		if [ -f $TEMPDIR/column_list_"$inputTable".out ] && [ -f $TEMPDIR/source_userview_"$inputTable".out ]
		then
		
		
			srcDataResInd=`grep '\*' $TEMPDIR/source_userview_"$inputTable".out  | wc -l`

		
			if [ "$srcDataResInd" -eq 0 ]
			then
		
				selectLineNo=`grep -i -w -n "SELECT" $TEMPDIR/source_userview_"$inputTable".out | grep -v '\-\-' | cut -f1 -d':' | head -1`
				
				if [ ! -z "$selectLineNo" ]
				then
				
					# echo "SELECT " >> $OUTDIR/custom_order_validation1.sql
					# paste -sd, $TEMPDIR/column_list_"$inputTable".out | sed 's/'DBC_COLUMN\,'//g' >> $OUTDIR/custom_order_validation1.sql
					# echo "FROM $prodUserView.$inputTable" >> $OUTDIR/custom_order_validation1.sql
					# echo " MINUS " >> $OUTDIR/custom_order_validation1.sql
					# echo "SELECT " >> $OUTDIR/custom_order_validation1.sql
					# paste -sd, $TEMPDIR/column_list_"$inputTable".out | sed 's/'DBC_COLUMN\,'//g' >> $OUTDIR/custom_order_validation1.sql
					# echo "FROM $prodUserView.$inputTable;" >> $OUTDIR/custom_order_validation1.sql
				
					# echo "SELECT TRIM(ColumnName) FROM DBC.ColumnsV WHERE TRIM(Database)='$prodUserView' AND TRIM(TableName)='$inputTable'" >> $OUTDIR/custom_order_validation2.sql
					# echo " MINUS SELECT TRIM(ColumnName) FROM DBC.ColumnsV WHERE TRIM(Database)='$prodUserView' AND TRIM(TableName)='T1108_$inputTable';" >> $OUTDIR/custom_order_validation2.sql
				
					# PART-1  Get Everything upto the SELECT CLAUSE
					head -$selectLineNo $TEMPDIR/source_userview_"$inputTable".out > $OUTDIR/userview_"$inputTable".out
									
					
					fromLineNo=`grep -i -w -n "FROM" $TEMPDIR/source_userview_"$inputTable".out | grep -v '\-\-' | cut -f1 -d':' | head -1`
					if [ ! -z "$fromLineNo" ]
					then
					
						columnDefLines=`expr $fromLineNo - $selectLineNo + 1`
						
					#PART-2 Format the order on everything between SELECT and FROM CLAUSES
						tail +$selectLineNo $TEMPDIR/source_userview_"$inputTable".out | head -$columnDefLines > $TEMPDIR/"$scriptName"_userview.out
						
						colOrder="0"

						# Read the columns from the target  view one after the other
						cat $TEMPDIR/column_list_"$inputTable".out | grep -v "DBC_COLUMN" | while read -r columnName; do
						
							colOrder=`expr $colOrder + 1`
						
									# Get the current column definition from source view. (with or without dr)
									grep -i -w -n $columnName $TEMPDIR/"$scriptName"_userview.out | grep -v '\-\-' \
									| grep -v -w -i "JOIN" | grep -v -w -i "FROM"  | grep -v -w -i "WHERE" | grep -v -w -i "ON" > $TEMPDIR/"$scriptName"_unique_list.out 
									
									lineCount=`cat $TEMPDIR/"$scriptName"_unique_list.out | cut -f2 -d':' | sort | uniq  | wc -l`
									
									if [ $lineCount -eq 1 ]
									then
										# Column Definition present in 1 line

										if [ $colOrder -gt 1 ]
										then
											# If column is not the first column remove keyword "SELECT" from the line
											perl -pi -e 's/'SELECT\ '/\,/gi'  $TEMPDIR/"$scriptName"_unique_list.out 
										fi
										cat $TEMPDIR/"$scriptName"_unique_list.out | cut -f2 -d':' | sort | uniq >> $OUTDIR/userview_"$inputTable".out
									else
									
										
										cat $TEMPDIR/"$scriptName"_unique_list.out | sort -t':' -nk 1,1 > $TEMPDIR/"$scriptName"_sorted_unique_list.out
										rangeFirst=`head -1 $TEMPDIR/"$scriptName"_sorted_unique_list.out | cut -f1 -d':'`
										rangeLast=`tail -1 $TEMPDIR/"$scriptName"_sorted_unique_list.out | cut -f1 -d':'`
																	
										if [ ! -z "$rangeFirst" ] && [ ! -z "$rangeLast" ]
										then
											# Column Definition Spans more than 1 line

											ind=`cat $TEMPDIR/"$scriptName"_userview.out | tail +$rangeFirst | head -1 | grep -i -w 'CASE' | wc -l`
											total=`expr $rangeLast - $rangeFirst + 1`
																			
											while [ $ind -eq 0 ] && [ $total -lt 5 ] && [ $rangeFirst -gt 1 ]
											do
												if [ $rangeFirst -gt 2 ]
												then
													rangeFirst=`expr $rangeFirst - 1`								
												fi
												ind=`cat $TEMPDIR/"$scriptName"_userview.out | tail +$rangeFirst | head -1 | grep -i -w 'CASE' | wc -l`
												total=`expr $rangeLast - $rangeFirst + 1`
											done
											
											if [  $total -gt 0  ] && [  $rangeFirst -gt 0  ] && [ $ind -gt 0 ]
											then
												echo  "," >> $OUTDIR/userview_"$inputTable".out
												cat $TEMPDIR/"$scriptName"_userview.out | tail +$rangeFirst | head -$total >> $OUTDIR/userview_"$inputTable".out
											else
												errorLineNo=`expr $rangeLast + $slectLineNo -1`
												echo "$inputTable,$columnName,$errorLineNo,Unable to get accurate column definition" >> $OUTDIR/custom_order_execptions.out
											fi
										
										else
										
											# Column Definition Not Found (Column in  View but not in source user view)
											echo ",ThisView.$columnName " >> $OUTDIR/userview_"$inputTable".out
											
											#echo "$inputTable,$columnName,,Unable to get accurate column definition" >> $OUTDIR/custom_order_execptions.out
										fi
									fi
							
						done
					
				
					# PART-3 Get Everything from the FROM line
						tail +$fromLineNo $TEMPDIR/source_userview_"$inputTable".out >> $OUTDIR/userview_"$inputTable".out
					
					else
						echo "$inputTable,,,Unable to find FROM CLAUSE" >> $OUTDIR/custom_order_execptions.out
					fi
				
				
					# Write the Script if no exceptions have been found
					if [ -f $OUTDIR/custom_order_execptions.out ]
					then
						errorInd=`grep -i -w  $inputTable $OUTDIR/custom_order_execptions.out | wc -l`
						if [ $errorInd -eq 0 ]
						then
							# SUCCESS - No Errors Found
							cat $OUTDIR/userview_"$inputTable".out | uniq >> $OUTDIR/custom_order.sql
						else
							cat $OUTDIR/userview_"$inputTable".out | uniq >> $OUTDIR/custom_order_error.sql
						fi
					else
						# SUCCESS - No Errors Found
						cat $OUTDIR/userview_"$inputTable".out | uniq >> $OUTDIR/custom_order.sql
					fi
					
				else
					echo "$inputTable,,,Unable to find SELECT CLAUSE" >> $OUTDIR/custom_order_execptions.out
				fi
		
			
			else
			
				# View is currently not a custom view in Source
		
		
			fi
		fi
		
	done
	
	
	
	
	
	