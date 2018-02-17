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
# STEP-3 Generate Scripts for Data Migration

	rm -f $SQLDIR/"$relName"_migrate_column_add_*.sql 
	rm -f $SQLDIR/"$relName"_migrate_simple_dtype_change_*.sql    # File does not need any analysis. Pre-Defined Conversions
	rm -f $SQLDIR/"$relName"_migrate_complex_dtype_change_*.sql    # File needs more analysis
	rm -f $SQLDIR/"$relName"_cutover_tpf_migrate_data.sql
	rm -f $SQLDIR/"$relName"_cutover_matviewdb_migrate_data.sql
	rm -f $SQLDIR/"$relName"_validation_check_for_dataconversion.sql

	rm -f $AUDITDIR/"$relName"_compare_migration_count.dat
	
	
	rm -f $TEMPDIR/"$relName"_getcol.sql
	rm -f $TEMPDIR/"$relName"_getcol_*.out
	rm -f $TEMPDIR/"$relName"_getcol.out
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,5,6 -d'|' | sort | uniq | while read -r line ; do      

		type=`echo $line | cut -f1 -d'|'`
		prodDB=`echo $line | cut -f2 -d'|'`
		table=`echo $line | cut -f3 -d'|'`
	
		if [ "$type" != "1" ] 		#  Existing Tables
		then
			echo ".EXPORT RESET;" >> $TEMPDIR/"$relName"_getcol.sql
			echo ".EXPORT REPORT FILE = $TEMPDIR/"$relName"_getcol_"$prodDB"_"$table".out;" >> $TEMPDIR/"$relName"_getcol.sql
			echo "SELECT ',' || TRIM(ColumnName) from DBC.Columns WHERE tableName='$table' AND DatabaseName='$prodDB';"  >> $TEMPDIR/"$relName"_getcol.sql
		fi
		
	done
	
	# Get the columns in production for all tables
	$SCRIPTDIR/epdba_runSQLFile.sh $TDPROD $TEMPDIR/"$relName"_getcol.sql $TEMPDIR/"$relName"_getcol.out | tee -a  $logFileName
	
	

	# Start Creating the conversion script for each table
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND TABLE ENTRY" | cut -f2,3,4,5,6,9,10 -d'|' | sort | uniq | while read -r line ; do      
		
		type=`echo $line | cut -f1 -d'|'`
		witsDB=`echo $line | cut -f2 -d'|'`
		tempDB=`echo $line | cut -f3 -d'|'`
		prodDB=`echo $line | cut -f4 -d'|'`
		table=`echo $line | cut -f5 -d'|'`
		migrationIndex=`echo $line | cut -f6 -d'|'`
		cutoverIndex=`echo $line | cut -f7 -d'|'`
		
		echo " Creating Conversion Script for $table ... "  >> $logFileName

		rm -f $TEMPDIR/"$scriptName"_getcol.sql
		rm -f $TEMPDIR/"$scriptName"_getcol.out
		rm -f $TEMPDIR/$scriptName-temp0.out
		rm -f $TEMPDIR/$scriptName-temp1.out
		rm -f $TEMPDIR/$scriptName-temp2.out
		rm -f $TEMPDIR/$scriptName-temp3.out
		
		
		
		if [ "$type" != "1" ] 		#  Existing Tables
		then

			if [ -f $TEMPDIR/"$relName"_getcol_"$prodDB"_"$table".out ]
			then
				sed '1,2d'  $TEMPDIR/"$relName"_getcol_"$prodDB"_"$table".out  > $TEMPDIR/$scriptName-temp0.out
		
				# Add " before and after each fieldName
				cat $TEMPDIR/$scriptName-temp0.out | while read -r fileLine; do
					var2=`echo $fileLine | awk '{print substr($0,2,length)}'`
					echo ",\"$var2\"" >> $TEMPDIR/$scriptName-temp1.out
				done

				echo "INSERT INTO /*$ticketNo*/ $tempDB.\"$table\""	>> $TEMPDIR/$scriptName-temp2.out
				echo "(" 						>> $TEMPDIR/$scriptName-temp2.out
				sed '1s/^.//' $TEMPDIR/$scriptName-temp1.out 		>> $TEMPDIR/$scriptName-temp2.out  # Delete the first character from first line (,)			
				echo ") " 						>> $TEMPDIR/$scriptName-temp2.out
				echo "SELECT "					>> $TEMPDIR/$scriptName-temp2.out

				
				rm -f $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out
				cat $DIR/$dbChgList | grep -w -i $table | grep -i "Column Datatype difference" > $TEMPDIR/"$scriptName"_manifest_changes_for_migration.tmp 
				cat $DIR/$dbChgList | grep -w -i $table | grep -i "Column Add" >> $TEMPDIR/"$scriptName"_manifest_changes_for_migration.tmp 
				cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.tmp | while read -r chgLine ;
				do
					calcInd=`echo $chgLine | cut -f6 -d'|'`
					
					if [ "$prodDB" == "$prodReportDB" ]
					then
						# Lead Reporting Table Changes
						if [ "$calcInd" != "Y" ] 
						then
							echo "$chgLine" >> $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out
						fi
					else
						# Calculated Reporting Table Changes
						if [ "$calcInd" == "Y" ]
						then
							echo "$chgLine" >> $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out
						fi
					fi
				done
				rm -f $TEMPDIR/"$scriptName"_manifest_changes_for_migration.tmp
				
				

				# Handle INT to VARCHAR and DECIMAL to VARCHAR
				cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out | grep -w -i "$table" | grep -i "Column Datatype difference" |  cut -f3,4,5 -d'|' | sort | uniq | while read -r fieldLine ; do

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
					
					
					flag0=`echo $oldType | grep -i "VARCHAR" | wc -l`
					flag1=`echo $newType | grep -i "INTEGER" | wc -l`
					flag2=`echo $newType | grep -i "DECIMAL" | wc -l`
					
					if [ $flag0 -ne 0 ] && [ $flag1 -ne 0 ]
					then
						perl -pi -e  's/"'$fieldName'"/CAST("'$fieldName'" AS INTEGER)/i'  $TEMPDIR/$scriptName-temp1.out					
					fi
					
					if [ $flag0 -ne 0 ] && [ $flag2 -ne 0 ]
					then
						convType=`echo $newType | cut -f1 -d')'`
						newconvType=""$convType")"
						perl -pi -e  's/"'$fieldName'"/CAST("'$fieldName'" AS "'$newconvType'")/i'  $TEMPDIR/$scriptName-temp1.out
					fi

				done


				sed '1s/^.//' $TEMPDIR/$scriptName-temp1.out 		>> $TEMPDIR/$scriptName-temp2.out		
				echo " FROM $prodDB.\"$table\";" 			>> $TEMPDIR/$scriptName-temp2.out
				echo " " 						>> $TEMPDIR/$scriptName-temp2.out

			
				rowCounter1=`cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -i "Column Datatype difference"  | wc -l` 		
				if [ $rowCounter1 -ne 0 ]
				then
					echo "/* Columns with Datatype changes " 	>> $TEMPDIR/$scriptName-temp2.out
					cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out |  tr '[a-z]' '[A-Z]' | grep -i "Column Datatype difference"  | grep -w -i $table | cut -f3,4,5 -d'|' >> $TEMPDIR/$scriptName-temp2.out
					echo "*/ " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
				fi


				rowCounter2=`cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -i 'COLUMN ADD'  | wc -l` 		
				if [ $rowCounter2 -ne 0 ]
				then
					echo "/* Columns added to table " 		>> $TEMPDIR/$scriptName-temp2.out
					cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out |  tr '[a-z]' '[A-Z]' | grep -i 'COLUMN ADD' | grep -w -i $table | cut -f3 -d'|' >> $TEMPDIR/$scriptName-temp2.out
					echo "*/ " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
				fi


				rowCounter3=`cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -v -i 'TABLE ADD' | grep -v -i 'COLUMN ADD' | grep -v -i 'Column Datatype difference'   | wc -l`
				if [ $rowCounter3 -ne 0 ]
				then
					echo "/* Columns without dataype changes " 	>> $TEMPDIR/$scriptName-temp2.out
					cat $TEMPDIR/"$scriptName"_manifest_changes_for_migration.out |  tr '[a-z]' '[A-Z]' | grep -w -i $table | grep -v -i 'TABLE ADD' | grep -v -i 'COLUMN ADD' | grep -v -i 'Column Datatype difference'  | cut -f3,4,5 -d'|' >> $TEMPDIR/$scriptName-temp2.out
					echo "*/ " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
					echo " " 					>> $TEMPDIR/$scriptName-temp2.out
				fi


				if [ "$type" == "2" ]        	# Tables with only column additions
				then							# 1 or 2 for Load Balancing
					cat $TEMPDIR/$scriptName-temp2.out >> $SQLDIR/"$relName"_migrate_column_add_"$migrationIndex".sql    
				fi

				if [ "$type" == "3" ]        	# Tables with simple datatype changes
				then							# 3 or 4 for Load Balancing
					cat $TEMPDIR/$scriptName-temp2.out >> $SQLDIR/"$relName"_migrate_simple_dtype_change_"$migrationIndex".sql
				fi

				if [ "$type" == "4" ]        	# Tables with complex datattype changes
				then							# 5 for Load Balancing
					cat $TEMPDIR/$scriptName-temp2.out >> $SQLDIR/"$relName"_migrate_complex_dtype_change_"$migrationIndex".sql
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
				cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND MAT VIEW TABLE ENTRY"  | grep -i -w $table | cut -f5,6,7 -d'|' | sort | uniq | while read -r line3 ; do

					reportTable=`echo $line3 | cut -f1 -d'|'`
					matViewDB=`echo $line3 | cut -f2 -d'|'`
					matViewTable=`echo $line3 | cut -f3 -d'|'`
					newTabName=$matViewDB.U_"$prefix"_"$matViewTable"

					rm -f $TEMPDIR/$scriptName-temp3.out

					sed -e 's/'$prodDB.\"$table\"'/'$newTabName'/g' $TEMPDIR/$scriptName-temp2.out > $TEMPDIR/$scriptName-temp3.out
					sed -e 's/'$tempDB.\"$table\"'/'$matViewDB.\"$matViewTable\"'/g' $TEMPDIR/$scriptName-temp3.out >> $SQLDIR/"$relName"_cutover_matviewdb_migrate_data.sql
					
				done
			
				echo "M|$prodDB|$table|$tempDB|$table" >> $AUDITDIR/"$relName"_compare_migration_count.dat

			else
				echo " $table Not Found in $prodDB" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			
			fi
		fi

	
	done
	
		# Load all the original and temp table names in a temp table for further analysis
	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_MIG_REPLACE_LIST  -d $AUDITDIR/"$relName"_compare_migration_count.dat -l $logFileName 

	