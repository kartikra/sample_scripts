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

	
# ksh epdba_process_custom_views.sh -t 2 -r TESTCO3 -i /users/q932624/dbmig/srcfiles/co_TESTCO3_new_dr.dat -o /users/q932624/dbmig/outfiles/co_TESTCO3_new_dr.sql > log2.log &
	
# STEP-3 Get Input Parameters

	while getopts t:r:w:i:o:p: par
        do      case "$par" in
                t)      runType="$OPTARG";;
                r)      region="$OPTARG";;
                w)      ticketNo="$OPTARG";;
                i)      inputFile="$OPTARG";;
                o)      outputFile="$OPTARG";;
				p)      projectReason="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh epdba_process_custom_views.sh -t <runType> -r <region> -w <ticketNo> -i <inputFile> -o <outputFile>"
                        exit 998;;
                esac
        done
	
	if [ -z "$projectReason" ]
	then
		projectReason=" 2014"
	fi
	
	
# STEP-4 Run Region Profile File

	if [ -z "$runType" ]
	then
		echo "Parameter $runType cannot be found, Exiting"
		exit 902
	fi


		
	
	# Get List of Data Restrictions
	if [ "$runType" == "1" ]
	then
	
		. $HOMEDIR/region/PROD_"$region".profile
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "Profile file PROD_"$region".profile cannot be found, Exiting"
			exit 902
		fi
		
		# STEP-5 Get the List of Views
	
		echo "SELECT  TRIM(TableName) || '|' || TRIM(DatabaseName) AS OBJ_LIST FROM DBC.TablesV " > $TEMPDIR/"$region"_list_custom_views.sql
		echo "WHERE TRIM(TableKind)='V' AND RequestText LIKE '%ROLE%IN%' " >> $TEMPDIR/"$region"_list_custom_views.sql
		echo "AND DatabaseName IN ('$prodUserView','prodTpfUserView') AND TRIM(TableName) NOT LIKE 'UPGR_%' AND TRIM(TableName) NOT LIKE '%_BAK' " >> $TEMPDIR/"$region"_list_custom_views.sql
		echo "AND TRIM(TableName) NOT LIKE 'U321_%'  " >> $TEMPDIR/"$region"_list_custom_views.sql
		echo " ORDER BY 1;" >> $TEMPDIR/"$region"_list_custom_views.sql
	
		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/"$region"_list_custom_views.sql $TEMPDIR/"$region"_list_custom_views.out | tee -a $logFileName
		sed '1d' $TEMPDIR/"$region"_list_custom_views.out > $TEMPDIR/"$region"_list_custom_views.tmp
		mv $TEMPDIR/"$region"_list_custom_views.tmp $TEMPDIR/"$region"_list_custom_views.out
		
		rm -f $TEMPDIR/get_dr_results.out
		
		cat $TEMPDIR/"$region"_list_custom_views.out | while read -r line ; do
		
			table=`echo $line | cut -f1 -d'|'`
			prodDB=`echo $line | cut -f2 -d'|'`

			echo "SHOW VIEW $prodDB.\"$table\";" > $TEMPDIR/get_custom_view.sql
			
			rm -f $TEMPDIR/get_custom_view.out
			$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/get_custom_view.sql $TEMPDIR/get_custom_view.out | tee -a $logFileName

			grep -i -w "ROLE" $TEMPDIR/get_custom_view.out | grep -i -w "IN"  > $TEMPDIR/get_dr_columns.out
	
			cat $TEMPDIR/get_dr_columns.out | while read -r columnLine ; do
				
				fieldPos1=`echo $columnLine | awk '{print index($0,"\ ROLE\ ")}'`
				fieldLen=`expr length "$columnLine"`
				
				modifiedLine=`echo $columnLine | cut -c $fieldPos1-$fieldLen`
				fieldPos2=`echo $modifiedLine | awk '{print index($0,"\ END\ ")}'`
				fieldLen=`expr length "$modifiedLine"`

				fieldName=`echo $modifiedLine | cut -c $fieldPos2-$fieldLen | sed 's/END//g' | sed 's/\ //g' | sed 's/\"//g'`
				fieldNameLen=`expr length "$fieldName"`
				
				rm -f $TEMPDIR/get_allcolumns.out
				if [ $fieldNameLen -gt 32 ]
				then
					fieldName="ALL COLUMNS"
					
					echo "SELECT TRIM(ColumnName) FROM DBC.ColumnsV WHERE TRIM(DatabaseName)='$prodUserView' AND TRIM(TableName)='$table'" > $TEMPDIR/get_allcolumns.sql
					$SCRIPTDIR/epdba_runSQLFile2.sh "$TDPROD" $TEMPDIR/get_allcolumns.sql $TEMPDIR/get_allcolumns.out | tee -a $logFileName
					
					sed '1d' $TEMPDIR/get_allcolumns.out > $TEMPDIR/get_allcolumns.tmp
					mv $TEMPDIR/get_allcolumns.tmp $TEMPDIR/get_allcolumns.out
					
				fi
				
				
				occurCount=$((`echo $modifiedLine | sed 's/[^\,]//g' | wc -c` - 1 ))
				endCount=`expr $occurCount + 1`
				i=1
				while [ i -le $endCount ]
				do
					roleName=`echo $modifiedLine | cut -f2 -d '(' | cut -f$i -d ',' | cut -f1 -d ')' | sed "s/'//g"`
					
					if [ ! -f "$TEMPDIR/get_allcolumns.out" ]
					then
						echo "$prodUserView,$table,$fieldName,$roleName," >> $TEMPDIR/get_dr_results.out
					else
						cat $TEMPDIR/get_allcolumns.out | while read -r fieldLine; do
							echo "$prodUserView,$table,$fieldLine,$roleName,Restriction added at view level" >> $TEMPDIR/get_dr_results.out
						done
					fi
					
					i=`expr $i + 1`
				done
			
			done
			
		done
		
		rm -f $OUTDIR/"$prodUserView"_restriction_list.csv	
		echo "DATABASE_NAME,VIEW_NAME,COLUMN_NAME,ROLENAME,COMMENTS" > $OUTDIR/"$prodUserView"_restriction_list.csv
		cat $TEMPDIR/get_dr_results.out | sort -t',' -k 1,1 -k 2,2  >> $OUTDIR/"$prodUserView"_restriction_list.csv
		
	fi	
		
	
	#--------------------------------------------------------------------------------------------------------------------------------------------------------
	# Apply New Data Restrictions
	
	
	if [ "$runType" == "2" ]
	then
	
		. $HOMEDIR/region/"$region".profile
		rt_cd=$?
		if [ $rt_cd -ne 0 ]
		then
			echo "Profile file PROD_"$region".profile cannot be found, Exiting"
			exit 902
		fi
	
	
		rm -f $outputFile
	
		cat $inputFile | cut -f1 -d'|' | sort | uniq | while read -r tableName ; do
		
			
			if [ ! -z "$tableName" ] 
			then
				cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$devUserView'/g' -e 's/'MY_TABLE'/'$tableName'/g' \
				-e 's/'MY_OUTDDL_FILE'/'"$scriptName"_"$tableName"_witsddl\.out'/g' -e 's/'MY_OBJECT'/'VIEW'/g'  > $TEMPDIR/"$scriptName"_"$devUserView"_getddl.sql
				
				$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/"$scriptName"_"$devUserView"_getddl.sql $TEMPDIR/"$scriptName"_"$tableName"_witsddl.out | tee -a  $logFileName
							
				if  [ -s $TEMPDIR/"$scriptName"_"$tableName"_witsddl.out ]
				then	
				
					rm -f $TEMPDIR/userview_column_list.dat
					noDrInd1=`grep -i "\*" $TEMPDIR/"$scriptName"_"$tableName"_witsddl.out | wc -l`
					noDrInd2=`grep -i -w "ROLE" $TEMPDIR/"$scriptName"_"$tableName"_witsddl.out | wc -l`

					# Check for Data Restrictions
					if [ $noDrInd1 -eq 0 ] && [ $noDrInd2 -ne 0 ]
					then
						grep -i -w -n "FROM" $TEMPDIR/"$scriptName"_"$tableName"_witsddl.out | grep -i "$devView" > $TEMPDIR/get_from_line.dat
						lineNo=`head -1 $TEMPDIR/get_from_line.dat | cut -f1 -d ':'`
						lastLineNo=`expr $lineNo - 1`
						head -$lastLineNo $TEMPDIR/"$scriptName"_"$tableName"_witsddl.out > $TEMPDIR/userview_column_list.dat
					fi
					
					rm -f $TEMPDIR/create_custom_view.sql
					rm -f $TEMPDIR/create_custom_view_dtl.sql
					
					cat $inputFile | grep -i -w $tableName | cut -f3,4,5,6 -d'|' | sort | uniq | while read -r line ; do
					
						resLevel=`echo $line | cut -f1 -d'|'`
						resType=`echo $line | cut -f2 -d'|'`
						joinTable=`echo $line | cut -f3 -d'|'`
						joinColumn=`echo $line | cut -f4 -d'|'`
						
						
						if [ "$resLevel" != "C" ]
						then
						
							if [ -s $TEMPDIR/userview_column_list.dat ]
							then
								cat $TEMPDIR/userview_column_list.dat > $TEMPDIR/create_custom_view.sql
							else
								changetTS=`date +%Y-%m-%d\ %H:%M:%S`
							
								echo "REPLACE VIEW $devUserView.$tableName AS LOCKING ROW FOR ACCESS" > $TEMPDIR/create_custom_view.sql
								echo "-- -------------------------------------------------------------" >> $TEMPDIR/create_custom_view.sql
								echo "-- Reason for Custom View  " >> $TEMPDIR/create_custom_view.sql
								echo "-- ------------------------------" >> $TEMPDIR/create_custom_view.sql
								echo "-- Row Level Data Restriction	" >> $TEMPDIR/create_custom_view.sql			
								echo "-- Change History " >> $TEMPDIR/create_custom_view.sql
								echo "-- ------------------------------ " >> $TEMPDIR/create_custom_view.sql
								echo "-- $changetTS : Accenture DBA Created Custom View for $projectReason " >> $TEMPDIR/create_custom_view.sql
								echo "-- ------------------------------ " >> $TEMPDIR/create_custom_view.sql
								echo "SELECT ThisView.* " >> $TEMPDIR/create_custom_view.sql
							fi
						
							rm -f $TEMPDIR/create_custom_view_dtl.sql
						
							if [ "$resType" == "F" ] && [ "$region" == "CO" ]
							then
								echo "FROM $devView.\"$tableName\" AS ThisView " > $TEMPDIR/create_custom_view_dtl.sql
								echo "INNER JOIN HCCLCO_METADATA.Financial_Security fView " >> $TEMPDIR/create_custom_view_dtl.sql
								echo "ON ThisView.SERVICE_AREA_ID = fView.SERV_AREA_ID " >> $TEMPDIR/create_custom_view_dtl.sql
								echo "AND fView.USR_ROLE = ROLE; " >> $TEMPDIR/create_custom_view_dtl.sql
							fi
							
							if [ "$resType" == "CL" ] && [ "$region" == "CO" ]
							then
								echo "FROM $devView.$tableName ThisView, " > $TEMPDIR/create_custom_view_dtl.sql
								echo "  $devView.Identity_Id iView, " >> $TEMPDIR/create_custom_view_dtl.sql
								echo "  HCCLCO_METADATA.Clinical_Security cView " >> $TEMPDIR/create_custom_view_dtl.sql
								echo " WHERE ThisView.PAT_ID = iView.PAT_ID " >> $TEMPDIR/create_custom_view_dtl.sql
								echo " AND iView.IDENTITY_TYPE_ID = cView.IDENTITY_TYPE_ID " >> $TEMPDIR/create_custom_view_dtl.sql
								echo " AND cView.USR_ROLE = ROLE; " >> $TEMPDIR/create_custom_view_dtl.sql
							fi
							
							resType1=`echo $resType | cut -f1 -d','`
							resType2=`echo $resType | cut -f2 -d','`
							if [ "$resType1" == "RCC" ] && [ "$resType2" == "BH" ]
							then
							
								cat $inputFile | grep -i -w $tableName | cut -f2,3,4 -d'|' | sort | uniq | while read -r line3 ; do
							
									resLevel=`echo $line3 | cut -f2 -d'|'`
									resType=`echo $line3 | cut -f3 -d'|'`
									resType1=`echo $resType | cut -f1 -d','`
									resType2=`echo $resType | cut -f2 -d','`
									if  [ "$resType1" == "RCC" ] && [ "$resType2" == "BH" ] && [ "$resLevel" == "R" ]
									then
										columnName=`echo $line3 | cut -f1 -d'|'`	
										echo "FROM $devView.\"$tableName\" ThisView " > $TEMPDIR/create_custom_view_dtl.sql
										echo "INNER JOIN $devUserView.\"$joinTable\" ccInView ON ThisView.\"$columnName\" = ccInView.\"$joinColumn\";" >> $TEMPDIR/create_custom_view_dtl.sql
									fi
								done
								
							fi
							

							if [ "$resType" == "C-IN" ] || [ "$resType" == "F-IN" ]
							then
							
								cat $inputFile | grep -i -w $tableName | cut -f2,3,4 -d'|' | sort | uniq | while read -r line3 ; do
							
									resLevel=`echo $line3 | cut -f2 -d'|'`
									resType=`echo $line3 | cut -f3 -d'|'`
									if [ "$resType" == "C-IN" ] || [ "$resType" == "F-IN" ]
									then
										columnName=`echo $line3 | cut -f1 -d'|'`	
										echo "FROM $devView.\"$tableName\" ThisView " > $TEMPDIR/create_custom_view_dtl.sql
										echo "INNER JOIN $devUserView.\"$joinTable\" ccInView ON ThisView.\"$columnName\" = ccInView.\"$joinColumn\";" >> $TEMPDIR/create_custom_view_dtl.sql
									fi
								done
								
							fi
							
							
							if [ "$resLevel" == "T" ]
							then
								echo "FROM $devView.\"$tableName\" AS ThisView " > $TEMPDIR/create_custom_view_dtl.sql
								echo "WHERE ROLE IN ('HCCLEXRRO','HCCLEXRRW','HCCLEXCRO','HCCLEXCRW','HCCLEXSRO','HCCLEXSRW'); " >> $TEMPDIR/create_custom_view_dtl.sql					
							fi
							
							
						else
							# Logic for Custom Views with Restriction at Column Level
							
							# Get List of Columns if not a custom view
							
							if [ ! -s $TEMPDIR/userview_column_list.dat ]
							then
								echo "SELECT ',ThisView.' || TRIM(ColumnName) from dbc.ColumnsV  WHERE DatabaseName='$devView' AND ColumnName IS NOT NULL AND TableName='$tableName'" > $TEMPDIR/get_columns1.sql
								$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/get_columns1.sql $TEMPDIR/get_columns1.dat | tee -a  $logFileName

								changetTS=`date +%Y-%m-%d\ %H:%M:%S`
							
								echo "REPLACE VIEW $devUserView.$tableName AS LOCKING ROW FOR ACCESS" > $TEMPDIR/create_custom_view.sql
								echo "-- -------------------------------------------------------------" >> $TEMPDIR/create_custom_view.sql
								echo "-- Reason for Custom View  " >> $TEMPDIR/create_custom_view.sql
								echo "-- ------------------------------" >> $TEMPDIR/create_custom_view.sql
								echo "-- Column Level Data Restriction added for " >> $TEMPDIR/create_custom_view.sql	

								cat $inputFile | grep -i -w $tableName | cut -f2,3,4 -d'|' | sort | uniq | while read -r line2 ; do
									columnName=`echo $line2 | cut -f1 -d'|'`
									echo "--	$columnName " >> $TEMPDIR/create_custom_view.sql
								done
								echo "-- Change History " >> $TEMPDIR/create_custom_view.sql
								echo "-- ------------------------------ " >> $TEMPDIR/create_custom_view.sql
								echo "-- $changetTS : Accenture DBA Created Custom View for $projectReason " >> $TEMPDIR/create_custom_view.sql
								echo "-- ------------------------------ " >> $TEMPDIR/create_custom_view.sql
								echo "SELECT " >> $TEMPDIR/create_custom_view.sql								

								# List of Columns That need to be added to view
								sed '1d'  $TEMPDIR/get_columns1.dat > $TEMPDIR/get_columns1.tmp
								sed  ''1's/\,/\ /g'  $TEMPDIR/get_columns1.tmp >> $TEMPDIR/create_custom_view.sql
								
								# List of Columns Added
								#echo "SELECT '--		' || TRIM(ColumnName) from dbc.ColumnsV  WHERE DatabaseName='$devView' AND ColumnName IS NOT NULL AND TableName='$tableName'" > $TEMPDIR/get_columns2.sql
								#$SCRIPTDIR/epdba_runSQLFile2.sh "$TDDEV" $TEMPDIR/get_columns2.sql $TEMPDIR/get_columns2.dat | tee -a  $logFileName						
								# sed '1d'  $TEMPDIR/get_columns2.dat > $TEMPDIR/get_columns2.tmp
								
							else
								cat $TEMPDIR/userview_column_list.dat > $TEMPDIR/create_custom_view.sql
							fi
							
							
							cat $inputFile | grep -i -w $tableName | cut -f2,3,4 -d'|' | sort | uniq | while read -r line2 ; do
							
								columnName=`echo $line2 | cut -f1 -d'|'`
								resLevel=`echo $line2 | cut -f2 -d'|'`
								resType=`echo $line2 | cut -f3 -d'|'`
							

								if [ "$resLevel" == "C" ]
								then
									string1=""
									string2=""
									string3=""
									string4=""
									string5=""	
								
									r1=`echo $resType | cut -f1 -d','`
									r2=`echo $resType | cut -f2 -d','`
									r3=`echo $resType | cut -f3 -d','`
									
								
									# Search and Replace Every Column Name
									string1=",CASE WHEN ROLE IN ("
									
									if [ "$r1" == "R" ] || [ "$r2" == "R" ] || [ "$r3" == "R" ]
									then
										if [ "$region" == "CO" ]
										then
											string2="'HCCL"$region"RRO','HCCL"$region"RRW','HCCLEXRRO','HCCLEXRRW'"
										else
											string2="'HCCL"$region"RRO','HCCL"$region"RRW'"
										fi
									fi
									
									if [ "$r1" == "S" ] || [ "$r2" == "S" ] || [ "$r3" == "S" ]
									then
										if [ ! -z "$string2" ]
										then
											string0=","
										else
											string0=""
										fi
										if [ "$region" == "CO" ]
										then
											string3=""$string0"'HCCL"$region"SRO','HCCL"$region"SRW','HCCLEXSRO','HCCLEXSRW'"
										else
											string3=""$string0"'HCCL"$region"SRO','HCCL"$region"SRW'"
										fi
									fi
									
									string5=") THEN ThisView.\"$columnName\" ELSE NULL END \"$columnName\""
									
									grep -i -w -n "$columnName" $TEMPDIR/create_custom_view.sql | grep -v "\-\-" > $TEMPDIR/get_line.dat
									position=`head -1 $TEMPDIR/get_line.dat | cut -f1 -d':'`
									if [ ! -z $position ]
									then
										sed ''$position'd' $TEMPDIR/create_custom_view.sql > $TEMPDIR/create_custom_view.tmp					
										awk -v n=$position -v s="$string1$string2$string3$string4$string5" 'NR == n {print s} {print }' $TEMPDIR/create_custom_view.tmp > $TEMPDIR/create_custom_view.sql
									else
										echo "Column $columnName not found in Table $tableName!!" >> logFileName
									fi
									#mv $TEMPDIR/create_custom_view.tmp $TEMPDIR/create_custom_view.sql
									
								fi
								
							done
							
						fi
					
					done
					
					if [ -s $TEMPDIR/create_custom_view_dtl.sql ]
					then
						cat $TEMPDIR/create_custom_view_dtl.sql >> $TEMPDIR/create_custom_view.sql
					else
						echo " FROM $devView.\"$tableName\" ThisView;" >> $TEMPDIR/create_custom_view.sql
					fi
					echo "" >> $TEMPDIR/create_custom_view.sql
					
					cat $TEMPDIR/create_custom_view.sql >> $outputFile
					
				else
					echo "Table Name $tableName  not found !!" >> logFileName
				fi
			else
				echo "Null Value for Table Name !!" >> logFileName
			fi
			
		
		done
	fi