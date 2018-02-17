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

		rm -f $HOME/test.dat
		rm -f $SQLDIR/tpf_staging_code.sql
		
create_tpf_storedProc()
{

	table=${1}
	storeProcVar=${2}
	inputStgDeployment=${3}
	subDeployStgDB=${4}
	subTpfReportDB=${5}
	subTpfDeployStgDB=${6}
	traceDeployId=${7}
	subDeployId=${8}
	traceCalcReportDB=${9}
	subTpfCalcReportDB=${10}
	subUserView=${11}
	TDHOST=${12}

	
	if [ -s $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat ]
	then
	

		if [ -z "$TDPROD" ]
		then
			storeProcDB=""$devStoredProcBaseName""$storeProcVar"_SP"
		else
			storeProcDB=""$prodStoredProcBaseName""$storeProcVar"_SP"   # Always Production Base
		fi
		
		
		
		rm -f $TEMPDIR/get_tpf_storeproc.dat
		
				# NOTE - If you want baseline to be something other than TDHOST, change TDHOST to approrpriate teradata region
		cat $SQLDIR/accdba_get_ddl.sql | sed -e 's/'MY_USER'/'$USER'/g' -e 's/'MY_DATABASE'/'$storeProcDB'/g' -e 's/'MY_TABLE'/'$table'/g' \
			-e 's/'MY_OUTDDL_FILE'/'get_tpf_storeproc\.dat'/g' -e 's/'MY_OBJECT'/'PROCEDURE'/g'  > $TEMPDIR/get_tpf_storeproc.sql
		$SCRIPTDIR/epdba_runSQLFile2.sh "$TDHOST" "$TEMPDIR/get_tpf_storeproc.sql" "$TEMPDIR/get_tpf_storeproc.dat" | tee -a  $logFileName
		
		cat $TEMPDIR/get_tpf_storeproc.dat >> $SQLDIR/"$relName"_backup_current_tpf_storeproc.sql
		
		# Position of the line containing the word BEGIN TRANSACTION
		lineNo=`cat $TEMPDIR/get_tpf_storeproc.dat | grep -i -w -n "TRANSACTION" | grep -i -w "BEGIN"  | cut -f1 -d ':'`
		
		if [ $region == "NC" ]
		then	
		
			if [ -s $TEMPDIR/get_tpf_storeproc.dat ] && [ ! -z "$lineNo" ]
			then
				# get everything till the line BEGIN TRANSACTION	
				head -$lineNo $TEMPDIR/get_tpf_storeproc.dat > $TEMPDIR/tpf_staging_code.dat
				
			else
				# Create code for Loading TPF Staging
				echo "REPLACE PROCEDURE $storeProcDB.$table()" > $TEMPDIR/tpf_staging_code.dat
				echo "BEGIN " >> $TEMPDIR/tpf_staging_code.dat
				cat $SQLDIR/accdba_tpf_stg_storeproc.sql | sed -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_TPF_STG'/'$subTpfDeployStgDB'/g' -e 's/'MY_STAGE_DB'/'$subDeployStgDB'/g' -e 's/'MY_USER_DB'/'$subUserView'/g' >> $TEMPDIR/tpf_staging_code.dat
				echo "BEGIN	TRANSACTION;" >> $TEMPDIR/tpf_staging_code.dat
			fi
			echo "END;" >> $TEMPDIR/tpf_staging_code.dat
			echo "ALTER PROCEDURE $storeProcDB."$table" COMPILE;" >> $TEMPDIR/tpf_staging_code.dat
		fi

		if [ $region == "NC" ]
		then
			echo "REPLACE PROCEDURE $storeProcDB.B_"$table"()" > $TEMPDIR/tpf_reporting_code.dat
			echo "BEGIN	" >> $TEMPDIR/tpf_reporting_code.dat
			echo "BEGIN	TRANSACTION;" >> $TEMPDIR/tpf_reporting_code.dat
		fi
		
		
		if [ $region == "SC" ]
		then
			if [ -s $TEMPDIR/get_tpf_storeproc.dat ] && [ ! -z "$lineNo" ]
			then
				# get everything till the line BEGIN TRANSACTION	
				head -$lineNo $TEMPDIR/get_tpf_storeproc.dat > $TEMPDIR/tpf_staging_code.dat
				
			else
				# Create code for Loading TPF Staging
				echo "REPLACE PROCEDURE $storeProcDB.$table()" > $TEMPDIR/tpf_staging_code.dat
				echo "BEGIN " >> $TEMPDIR/tpf_staging_code.dat
				cat $SQLDIR/accdba_tpf_stg_storeproc.sql | sed -e 's/'MY_TABLE'/'$table'/g' -e 's/'MY_TPF_STG'/'$subTpfDeployStgDB'/g' -e 's/'MY_STAGE_DB'/'$subDeployStgDB'/g' -e 's/'MY_USER_DB'/'$subUserView'/g' >> $TEMPDIR/tpf_staging_code.dat
				echo "BEGIN	TRANSACTION;" >> $TEMPDIR/tpf_staging_code.dat
			fi
		fi
		
		
		# Replace the DatabaseName in the script if not running in Prod
		if [ -z "$TDPROD" ]
		then
			perl -pi -e 's/'$prodUserView'/'$devUserView'/gi'  $TEMPDIR/tpf_staging_code.dat 
			
			case "$storeProcVar" in
				"A")
					perl -pi -e 's/'$prodTpfDeployStgDB1'/'$devTpfDeployStgDB1'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB1'/'$devDeployStgDB1'/gi'  $TEMPDIR/tpf_staging_code.dat 
				;;
				"B")
					perl -pi -e 's/'$prodTpfDeployStgDB2'/'$devTpfDeployStgDB2'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB2'/'$devDeployStgDB2'/gi'  $TEMPDIR/tpf_staging_code.dat 	
				;;
				"C")
					perl -pi -e 's/'$prodTpfDeployStgDB3'/'$devTpfDeployStgDB3'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB3'/'$devDeployStgDB3'/gi'  $TEMPDIR/tpf_staging_code.dat 	
				;;
				"D")
					perl -pi -e 's/'$prodTpfDeployStgDB4'/'$devTpfDeployStgDB4'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB4'/'$devDeployStgDB4'/gi'  $TEMPDIR/tpf_staging_code.dat 	
				;;
				"E")
					perl -pi -e 's/'$prodTpfDeployStgDB5'/'$devTpfDeployStgDB5'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB5'/'$devDeployStgDB5'/gi'  $TEMPDIR/tpf_staging_code.dat 	
				;;
				"F")
					perl -pi -e 's/'$prodTpfDeployStgDB6'/'$devTpfDeployStgDB6'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB6'/'$devDeployStgDB6'/gi'  $TEMPDIR/tpf_staging_code.dat 	
				;;
				"G")
					perl -pi -e 's/'$prodTpfDeployStgDB7'/'$devTpfDeployStgDB7'/gi'  $TEMPDIR/tpf_staging_code.dat 
					perl -pi -e 's/'$prodDeployStgDB7'/'$devDeployStgDB7'/gi'  $TEMPDIR/tpf_staging_code.dat 	
				;;
			esac
		fi		
		
		
		
		# Put the ETL Trace Code as Reportimg Code
		cat $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat >> $TEMPDIR/tpf_reporting_code.dat

		# Replace the DatabaseName in the script
		perl -pi -e 's/'$inputStgDeployment'/'$subDeployStgDB'/gi'  $TEMPDIR/tpf_reporting_code.dat 
		perl -pi -e 's/'$devReportDB'/'$subTpfReportDB'/gi'  $TEMPDIR/tpf_reporting_code.dat 
		perl -pi -e 's/'$traceCalcReportDB'/'$subTpfCalcReportDB'/gi'  $TEMPDIR/tpf_reporting_code.dat 
		perl -pi -e 's/'$traceDeployId'/'$subDeployId'/gi'  $TEMPDIR/tpf_reporting_code.dat 

		#grep -n -w -i "$table" $TEMPDIR/tpf_reporting_code.dat  | grep -v "$subTpfReportDB" | grep -v "$subTpfCalcReportDB"  > $TEMPDIR/tpf_reporting_code_line.dat
		#lastLineNo=`tail -1 $TEMPDIR/tpf_reporting_code_line.dat | cut -f1 -d':'`
		# # Remove " from the last line
		# sed  ''$lastLineNo's/\"//g'  $TEMPDIR/tpf_reporting_code.dat > $TEMPDIR/tpf_reporting_code.tmp
		# mv $TEMPDIR/tpf_reporting_code.tmp $TEMPDIR/tpf_reporting_code.dat
		
		#perl -pi -e 's/'$subDeployStgDB'\.\ /'$subDeployStgDB'\./gi'  $TEMPDIR/tpf_reporting_code.dat 
		#perl -pi -e 's/'$subDeployStgDB'\.\'\t'/'$subDeployStgDB'\./gi' $TEMPDIR/tpf_reporting_code.dat

		
		perl -pi -e 's/\"'$subDeployStgDB'\"\.\"'$table'\"/\"'$subTpfDeployStgDB'\"\.\"'$table'\"/gi'  $TEMPDIR/tpf_reporting_code.dat 

		echo "END TRANSACTION;" >> $TEMPDIR/tpf_reporting_code.dat
		echo "END;" >> $TEMPDIR/tpf_reporting_code.dat
		
		if [ $region == "NC" ]
		then
			echo "ALTER PROCEDURE $storeProcDB.B_"$table" COMPILE;" >> $TEMPDIR/tpf_reporting_code.dat
		fi
		if [ $region == "SC" ]
		then
			echo "ALTER PROCEDURE $storeProcDB."$table" COMPILE;" >> $TEMPDIR/tpf_reporting_code.dat
		fi
		
		perl -pi -e 's/'CREATE\ PROCEDURE'/'REPLACE\ PROCEDURE'/gi'  $TEMPDIR/tpf_staging_code.dat
		perl -pi -e 's/'CREATE\ PROCEDURE'/'REPLACE\ PROCEDURE'/gi'  $TEMPDIR/tpf_reporting_code.dat 

		
		# Clecnup OLD CRQ Numbers from Stg
		mv $TEMPDIR/tpf_staging_code.dat $SQLDIR/tpf_staging_code.sql
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR tpf_staging_code.sql 
		sed -e 's/[Pp][Rr][Oo][Cc][Ee][Dd][Uu][Rr][Ee]\ /PROCEDURE\ \/\*'$ticketNo'\*\//' $SQLDIR/tpf_staging_code.sql >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		rm -f $SQLDIR/tpf_staging_code.sql
		
		# Clecnup OLD CRQ Numbers from Rpt
		mv $TEMPDIR/tpf_reporting_code.dat $SQLDIR/tpf_reporting_code.sql
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR tpf_reporting_code.sql
		sed -e 's/[Pp][Rr][Oo][Cc][Ee][Dd][Uu][Rr][Ee]\ /PROCEDURE\ \/\*'$ticketNo'\*\//' $SQLDIR/tpf_reporting_code.sql >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		rm -f $SQLDIR/tpf_reporting_code.sql
		
		echo "" >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		echo "" >> $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
		
		

		
	fi
	
}		


relName="nc0611"
ticketNo="WO0000004010495"
regionProfile="NCAL_WITS3"


	USR_PROF=$HOMEDIR/region/$regionProfile.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file $regionProfile.profile cannot be found, Exiting"
                exit 902
		else
			. $HOMEDIR/region/PROD_"$region".profile
			#. $HOMEDIR/region/PROD_VALID_SC.profile
			rt_cd=$?
			if [ $rt_cd -ne 0 ]
			then
                echo "Profile file PROD_"$region".profile cannot be found, Exiting"
                exit 902
			fi
        fi

		

#----------------------------------------------------------------------------------------------------------------------------------------#
# STEP-11 Create TPF Stored Procedures	

	#--------------------------------------------------------------------------------
	# Input File must end with SRC; and have "  
	# Example -
	# FROM	"HCCLDSC9E_RESC_S"."IMMUNE" SRC;
	#--------------------------------------------------------------------------------
	rm -f $OUTDIR/"$relName"_tpf_store_proc_analysis.dat
	if [ -f $DIR/"$relName"_trace_code.txt ]
	then
	
		if [ ! -d $SQLDIR/archive/"$relName"/traceCode ]
		then
			mkdir $SQLDIR/archive/"$relName"/traceCode
		fi
		
		startPos="1"
		grep -i -n "SRC\;" $DIR/"$relName"_trace_code.txt | cut -f1 -d":" | while read -r line; do

			endPos=$line
			
			sed -n ''$startPos','$endPos' p' $DIR/"$relName"_trace_code.txt  > $TEMPDIR/trace_code.tmp
			
			inputStgDeployment=`tail -1 $TEMPDIR/trace_code.tmp | cut -f2 -d'"'`
			table=`tail -1 $TEMPDIR/trace_code.tmp | cut -f4 -d'"'`

			mv $TEMPDIR/trace_code.tmp  $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat
			
			startPos=`expr $endPos + 1`
			echo "$table|$inputStgDeployment" >> $OUTDIR/"$relName"_tpf_store_proc_analysis.dat
			
		done
	fi

	rm -f $SQLDIR/"$relName"_cutover_tpf_storeproc_changes.sql
	rm -f $SQLDIR/"$relName"_backup_current_tpf_storeproc.sql
	
	cat $OUTDIR/"$relName"_migration_analysis.out | grep "SCRIPT FOUND IMPACTED TPF STORED PROCEDURE" | cut -f3 -d'|' | sort | uniq | while read -r tabLine ; do
	
		cat $OUTDIR/"$relName"_tpf_store_proc_analysis.dat | grep -i -w "$tabLine" > $TEMPDIR/tpf_store_proc_analysis.dat
		
		if [ -s $TEMPDIR/tpf_store_proc_analysis.dat ] && [ -s $SQLDIR/archive/"$relName"/traceCode/sp_"$tabLine".dat ]
		then
			table=`head -1 $TEMPDIR/tpf_store_proc_analysis.dat | cut -f1 -d'|'`
			inputStgDeployment=`head -1 $TEMPDIR/tpf_store_proc_analysis.dat | cut -f2 -d'|'`
			
			case "$inputStgDeployment" in
				"$devDeployStgDB1")
					traceDeployId=$deployId1
					traceCalcReportDB=$devCalcReportDB1
				;;
				"$devDeployStgDB2")
					traceDeployId=$deployId2
					traceCalcReportDB=$devCalcReportDB2
				;;
				"$devDeployStgDB3")
					traceDeployId=$deployId3
					traceCalcReportDB=$devCalcReportDB3
				;;
				"$devDeployStgDB4")
					traceDeployId=$deployId4
					traceCalcReportDB=$devCalcReportDB4
				;;
				"$devDeployStgDB5")
					traceDeployId=$deployId5
					traceCalcReportDB=$devCalcReportDB5
				;;
				"$devDeployStgDB6")
					traceDeployId=$deployId6
					traceCalcReportDB=$devCalcReportDB6
				;;
				"$devDeployStgDB7")
					traceDeployId=$deployId7
					traceCalcReportDB=$devCalcReportDB7
				;;
			esac
			
			
			echo "$inputStgDeployment $devDeployStgDB1 $devTpfReportDB $devTpfDeployStgDB1 $traceDeployId $deployId1 $traceCalcReportDB $devTpfCalcReportDB1 $devUserView  $TDDEV"
			
			# Add all checks - check for table and stagingDatabase
			check1=`cat $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat | grep -i $inputStgDeployment | wc -l`
			check2=`cat $SQLDIR/archive/"$relName"/traceCode/sp_"$table".dat | grep -i $table | wc -l`
			if [ "$check1" -gt 0 ] && [ "$check2" -gt 0 ]
			then
				create_tpf_storedProc $table "A" $inputStgDeployment $devDeployStgDB1 $devTpfReportDB $devTpfDeployStgDB1 $traceDeployId $deployId1 $traceCalcReportDB $devTpfCalcReportDB1 $devUserView  $TDDEV
				create_tpf_storedProc $table "B" $inputStgDeployment $devDeployStgDB2 $devTpfReportDB $devTpfDeployStgDB2 $traceDeployId $deployId2 $traceCalcReportDB $devTpfCalcReportDB2 $devUserView  $TDDEV
				create_tpf_storedProc $table "C" $inputStgDeployment $devDeployStgDB3 $devTpfReportDB $devTpfDeployStgDB3 $traceDeployId $deployId3 $traceCalcReportDB $devTpfCalcReportDB3 $devUserView  $TDDEV
				create_tpf_storedProc $table "D" $inputStgDeployment $devDeployStgDB4 $devTpfReportDB $devTpfDeployStgDB4 $traceDeployId $deployId4 $traceCalcReportDB $devTpfCalcReportDB4 $devUserView  $TDDEV
				create_tpf_storedProc $table "E" $inputStgDeployment $devDeployStgDB5 $devTpfReportDB $devTpfDeployStgDB5 $traceDeployId $deployId5 $traceCalcReportDB $devTpfCalcReportDB5 $devUserView  $TDDEV
				create_tpf_storedProc $table "F" $inputStgDeployment $devDeployStgDB6 $devTpfReportDB $devTpfDeployStgDB6 $traceDeployId $deployId6 $traceCalcReportDB $devTpfCalcReportDB6 $devUserView  $TDDEV
				#create_tpf_storedProc $table "G" $inputStgDeployment $devDeployStgDB7 $devTpfReportDB $devTpfDeployStgDB7 $traceDeployId $deployId7 $traceCalcReportDB $devTpfCalcReportDB7 $devUserView 
			else
				echo "Trace Code for $tabLine does not appear to be valid" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
			fi
		else
			echo "Trace Code for $tabLine was not found" >> $SQLDIR/"$relName"_script_generated_exceptions.sql
		fi
	
	done
	