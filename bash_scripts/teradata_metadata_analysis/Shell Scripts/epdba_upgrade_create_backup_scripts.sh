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

	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Preparing Scripts for Backup -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	
	# relName="ga0906"
	# ticketNo="CRQ000000233033"
    # regionProfile="REGNGAB"

	
	relName=$1
	ticketNo=$2
	regionProfile=$3
	
	. $SQLDIR/archive/"$relName"/runid.profile

	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	backupPrefix="U$prefix"

	
	# STEP-2 Run Region Profile File
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

create_backup_view () {	
		
	in_view=$1
	out_file=$2
	
		
	echo "SHOW VIEW $in_view;" > $TEMPDIR/get_view.sql
	$SCRIPTDIR/epdba_runSQLFile2.sh $TDPROD $TEMPDIR/get_view.sql $TEMPDIR/get_view.out | tee -a  $logFileName

	in_db=`echo $in_view | cut -f1 -d'.'`
	viewName=`echo $in_view | cut -f2 -d '.'`
	
	perl -pi -e 's/'CREATE\ *VIEW'/'REPLACE\ VIEW'/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'CV\ *HCCL'/'REPLACE\ VIEW\ HCCL'/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'CV\ *\"*HCCL'/'REPLACE\ VIEW\ \"HCCL'/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'CV\ *KPBI'/'REPLACE\ VIEW\ KPBI'/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'CV\ *\"*KPBI'/'REPLACE\ VIEW\ \"KPBI'/gi'  $TEMPDIR/get_view.out 
	

	# Shorten Tablename if greater than 22 characters in length
	if [ -f $SQLDIR/archive/$relName/backupdb_tablename_replace.list ]
	then
		cat $SQLDIR/archive/$relName/backupdb_tablename_replace.list | sort | uniq | while read -r tabLine ; do
			dbName=`echo $tabLine | cut -f1 -d '|'`
			
			grep -i -w -n  "$dbName" $TEMPDIR/get_view.out > $TEMPDIR/get_cols.tmp
			cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
					sed  ''$lineNumber's/\"//g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
					mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
			done
			
			findWord=`echo $tabLine | cut -f2 -d '|'`
			replaceWord=`echo $tabLine | cut -f3 -d '|'`
			
			perl -pi -e 's/'$dbName'\.'$findWord'/'$dbName'\.'$replaceWord'/gi'  $TEMPDIR/get_view.out 
			perl -pi -e 's/'$dbName'\.\ '$findWord'/'$dbName'\.'$replaceWord'/gi'  $TEMPDIR/get_view.out 

		done
	fi
	
	
	# Check if Length is greater than 25. If yes then take the first 25 characters from the original view name
	viewNameLen=`expr length "$viewName"`
	if [ $viewNameLen -gt 25 ]
	then
		upgrViewName=`echo $viewName | awk '{print substr($0,1,25)}'`
		perl -pi -e 's/'$in_db'\.'$viewName'/'$in_db'\.'$upgrViewName'/gi'  $TEMPDIR/get_view.out 
		perl -pi -e 's/'$in_db'\.\ '$viewName'/'$in_db'\.'$upgrViewName'/gi'  $TEMPDIR/get_view.out 
	else
		upgrViewName=$viewName
	fi
	echo "V|"$in_db"|"$viewName"|"$in_db"|UPGR_"$upgrViewName"" >> $AUDITDIR/"$relName"_compare_backup_count.dat
	
	
	# Remove Tab or Space after Target Database Name
	perl -pi -e 's/'$in_db'\.\'\t'/'$in_db'\./gi' $TEMPDIR/get_view.out
	perl -pi -e 's/'$in_db'\.\ /'$in_db'\./gi' $TEMPDIR/get_view.out	
	
	
	if  [ "$in_db" == "$prodTpfView" ] || [ "$in_db" == "$prodTpfUserView" ]
	then
								# TPF Replacement

		# Replace TPF Reporting Databases
		cat $SQLDIR/archive/$relName/tpf_backupdb_replace.list | while read -r chgList ; do
		
			findWord=`echo $chgList | cut -f1 -d '|'`
			replaceWord=`echo $chgList | cut -f2 -d '|'`
			backupTabPrefix=`echo $chgList | cut -f4 -d '|'`
			
			grep -i -w -n  "$findWord" $TEMPDIR/get_view.out > $TEMPDIR/get_cols.tmp
			cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
					sed  ''$lineNumber's/\"//g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
					mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
			done
	
			foundInd=`cat $TEMPDIR/get_view.out | grep -i -w "$findWord"  | wc -l`
			if [ $foundInd -gt 0 ]
			then
				if [ ! -z "$findWord" ]
				then
					# Remove Tab or Space after Source Database Name
					perl -pi -e 's/'$findWord'\.\'\t'/'$findWord'\./gi' $TEMPDIR/get_view.out
					perl -pi -e 's/'$findWord'\.\ /'$findWord'\./gi' $TEMPDIR/get_view.out	
				fi
				
				if [ ! -z "$replaceWord" ]
				then
					# Replace Database Name with Archive DBname. Also add the prefix to the tablename
					perl -pi -e 's/'$findWord'/'$replaceWord'/gi'  $TEMPDIR/get_view.out 
					perl -pi -e 's/'$replaceWord'\.\ /'$replaceWord'\./gi'  $TEMPDIR/get_view.out 

					if [ -z "$backupTabPrefix" ]
					then
						perl -pi -e 's/'$replaceWord'\./'$replaceWord'\.'$backupPrefix'\_/gi'  $TEMPDIR/get_view.out 
					else
						perl -pi -e 's/'$replaceWord'\./'$replaceWord'\.'$backupPrefix'\_'$backupTabPrefix'/gi'  $TEMPDIR/get_view.out 
					fi
				fi
			fi
			
		done
		
		# Replace Reporting Views
		grep -i -w -n  "$prodTpfView" $TEMPDIR/get_view.out > $TEMPDIR/get_cols.tmp
		grep -i -w -n  "$prodMatView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
		grep -i -w -n  "$prodKPBIView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
		grep -i -w -n  "$prodTpfUserView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
		
		cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
					sed  ''$lineNumber's/\"//g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
					mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
		done
		
		perl -pi -e 's/'$prodTpfView'\./'$prodTpfView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
		perl -pi -e 's/'$prodTpfUserView'\./'$prodTpfUserView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
	
	else
								# Non-TPF Replacement

		# Replace Reporting Databases
		cat $SQLDIR/archive/$relName/backupdb_replace.list | while read -r chgList ; do
		
			findWord=`echo $chgList | cut -f1 -d '|'`
			replaceWord=`echo $chgList | cut -f2 -d '|'`
			backupTabPrefix=`echo $chgList | cut -f4 -d '|'`
			
			grep -i -w -n  "$findWord" $TEMPDIR/get_view.out > $TEMPDIR/get_cols.tmp
			cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
					sed  ''$lineNumber's/\"//g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
					mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
			done
	
			foundInd=`cat $TEMPDIR/get_view.out | grep -i -w "$findWord"  | wc -l`
			if [ $foundInd -gt 0 ]
			then
			
				if [ ! -z "$findWord" ]
				then
					# Remove Tab or Space after Source Database Name
					perl -pi -e 's/'$findWord'\.\'\t'/'$findWord'\./gi' $TEMPDIR/get_view.out
					perl -pi -e 's/'$findWord'\.\ /'$findWord'\./gi' $TEMPDIR/get_view.out	
				fi
				
				if [ ! -z "$replaceWord" ]
				then
					# Replace Database Name with Archive DBname. Also add the prefix to the tablename
					perl -pi -e 's/'$findWord'/'$replaceWord'/gi'  $TEMPDIR/get_view.out 
					perl -pi -e 's/'$replaceWord'\.\ /'$replaceWord'\./gi'  $TEMPDIR/get_view.out 

					if [ -z "$backupTabPrefix" ]
					then
						perl -pi -e 's/'$replaceWord'\./'$replaceWord'\.'$backupPrefix'\_/gi'  $TEMPDIR/get_view.out 
					else
						perl -pi -e 's/'$replaceWord'\./'$replaceWord'\.'$backupPrefix'\_'$backupTabPrefix'/gi'  $TEMPDIR/get_view.out 
					fi
				fi
				
			fi
			
		done
		
		# Replace Reporting Views
		grep -i -w -n  "$prodView" $TEMPDIR/get_view.out > $TEMPDIR/get_cols.tmp
		grep -i -w -n  "$prodMatView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
		grep -i -w -n  "$prodKPBIView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
		grep -i -w -n  "$prodUserView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
		
		cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
					sed  ''$lineNumber's/\"//g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
					mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
		done
		
		# Make all the View Repalcements for UPGR_
		perl -pi -e 's/'$prodView'\./'$prodView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
		perl -pi -e 's/'$prodMatView'\./'$prodMatView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
		perl -pi -e 's/'$prodKPBIView'\./'$prodKPBIView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
		perl -pi -e 's/'$prodUserView'\./'$prodUserView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
		
	fi
	
	
	# Replace any space or tab after UPGR_
	sed  -e 's/'UPGR\_\ '/'UPGR\_'/g' $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
	mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
	
	cat $TEMPDIR/get_view.out  >> $out_file
	
}
	

	
	
	#--------------------------- Main Program Starts Here ------------------------
	
	
	
	#-----     Before Running this shell script make sure that replacement list is up to date
	if [ ! -f $SQLDIR/archive/$relName/backupdb_replace.list ]
	then
		echo "Database Replacement List not found !! "
		exit 901
	fi
	if [ ! -f $SQLDIR/archive/$relName/backup_ddl.list ]
	then
		echo "Backup Database DDL List not found !! "
		exit 902
	fi
	
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/accdba_backup_analysis_collect_results.sql $OUTDIR/"$relName"_backup_analysis_collect_results.out | tee -a  $logFileName

	
	#--------------------------- Scripts for Backing Up DDL of all impacted databases ------------------------

	nohup $SCRIPTDIR/epdba_export_ddl.sh -h $TDPROD -l $SQLDIR/archive/$relName/backup_ddl.list &
	echo "Running DDL Backup Scripts in Background ...." >> $logFileName

	
	#-------------------------------------------------------------
	# Create Audit Files for the following -
	# 1) Source Table vs Backup Table
	# 2) Source View vs Backup View
	# 3) Reasonable Volume pre-etl and post-etl
	#-------------------------------------------------------------
	rm -f $AUDITDIR/"$relName"_source_table_count.dat
	rm -f $AUDITDIR/"$relName"_backup_table_count.dat
	rm -f $AUDITDIR/"$relName"_original_view_count.dat
	rm -f $AUDITDIR/"$relName"_upgr_view_count.dat
	rm -f $AUDITDIR/"$relName"_reasonable_volume_pre_count.dat
	rm -f $AUDITDIR/"$relName"_reasonable_volume_post_count.dat
	rm -f $AUDITDIR/"$relName"_compare_backup_count.dat
	
	
	echo "Audit Files have been created !! " >> $logFileName

	
	#--------------------------- Create Scripts for Backing Up Tables ------------------------
	echo "Creating Scripts for Backing up Tables ...." >> $logFileName

	rm -f $SQLDIR/"$relName"_backup_create_tables.sql
	rm -f $SQLDIR/"$relName"_backup_delete_data_from_backup_tables.sql
	rm -f $SQLDIR/"$relName"_backup_enable_blocklevel_compression.sql
	rm -f $SQLDIR/"$relName"_backup_migrate_data_from_prod_tables.sql
	rm -f $SQLDIR/archive/$relName/backupdb_tablename_replace.list

	echo "SET QUERY_BAND='BlockCompression=YES;' FOR SESSION;" > $SQLDIR/"$relName"_backup_migrate_data_from_prod_tables.sql
	
	cat $OUTDIR/"$relName"_backup_analysis_collect_results.out | grep -i "Found Backup Table" | while read -r line ; do
	
		dbName=`echo $line | cut -f2 -d'|'`
		tabName=`echo $line | cut -f3 -d'|'`
		backupTabPrefix=""  	# Initalize at the start of each iteration
		tpfbackupTabPrefix="" 	# Initalize at the start of each iteration
		
		# Check if Length is greater than 22. If yes then take the first 22 characters from the original table name
		tableNameLen=`expr length "$tabName"`
		if [ $tableNameLen -gt 22 ]
		then
			backupTabName=`echo $tabName | awk '{print substr($0,1,22)}'`
			echo "$dbName|$tabName|$backupTabName" >> $SQLDIR/archive/$relName/backupdb_tablename_replace.list
		else
			backupTabName=$tabName
		fi
				
		if [ -f "$SQLDIR/archive/$relName/tpf_backupdb_replace.list" ]
		then
			tpfbackupDB=`cat $SQLDIR/archive/$relName/tpf_backupdb_replace.list | grep -w -i $dbName | head -1 | cut -f2 -d'|'`
			tpfbackupTabPrefix=`cat $SQLDIR/archive/$relName/tpf_backupdb_replace.list | grep -w -i $dbName | head -1 | cut -f4 -d'|'`
			if [ -z "$tpfbackupDB" ]
			then
				backupDB=`cat $SQLDIR/archive/$relName/backupdb_replace.list | grep -w -i $dbName | head -1 | cut -f2 -d'|'`
				backupTabPrefix=`cat $SQLDIR/archive/$relName/backupdb_replace.list | grep -w -i $dbName | head -1 | cut -f4 -d'|'`
			else
				backupDB=$tpfbackupDB
				backupTabPrefix=$tpfbackupTabPrefix
			fi
		else
			backupDB=`cat $SQLDIR/archive/$relName/backupdb_replace.list | grep -w -i $dbName | head -1 | cut -f2 -d'|'`
			backupTabPrefix=`cat $SQLDIR/archive/$relName/backupdb_replace.list | grep -w -i $dbName | head -1 | cut -f4 -d'|'`
		fi
		
	
		if [ -z "$backupDB" ] 
		then
			echo "Backp Database Not found in replacement list for $dbName"
		else
			
			echo "CREATE TABLE /*$ticketNo*/ $backupDB.\""$backupPrefix"_"$backupTabPrefix""$backupTabName"\",NO FALLBACK AS $dbName.\"$tabName\" WITH DATA AND STATS; \
			DELETE FROM /*$ticketNo*/ $backupDB.\""$backupPrefix"_"$backupTabPrefix""$backupTabName"\" ALL;" >> $SQLDIR/"$relName"_backup_create_tables.sql
			
			#echo "DELETE FROM /*$ticketNo*/ $backupDB.\""$backupPrefix"_"$backupTabPrefix""$backupTabName"\" ALL;" >> $SQLDIR/"$relName"_backup_delete_data_from_backup_tables.sql
			
			echo "ALTER TABLE /*$ticketNo*/ $backupDB.\""$backupPrefix"_"$backupTabPrefix""$backupTabName"\", BLOCKCOMPRESSION = MANUAL;" >> $SQLDIR/"$relName"_backup_enable_blocklevel_compression.sql

			echo "INSERT INTO /*$ticketNo*/ $backupDB.\""$backupPrefix"_"$backupTabPrefix""$backupTabName"\" SELECT \\* FROM $dbName.\"$tabName\";" >> $SQLDIR/"$relName"_backup_migrate_data_from_prod_tables.sql

			echo ""$dbName"|"$tabName"" >> $AUDITDIR/"$relName"_source_table_count.dat
			echo ""$backupDB"|"$backupPrefix"_"$backupTabPrefix""$backupTabName"" >> $AUDITDIR/"$relName"_backup_table_count.dat
		
			echo "T|"$dbName"|"$tabName"|"$backupDB"|"$backupPrefix"_"$backupTabPrefix""$backupTabName"" >> $AUDITDIR/"$relName"_compare_backup_count.dat
			
		fi
	
	done
	
	echo "Completed Scripts for Backing up Tables !! " >> $logFileName


	
	
	#--------------------------- Create Scripts for Backing Up Views ------------------------
	echo "Creating Scripts for Backing up Views ...." >> $logFileName

	
	rm -f $SQLDIR/"$relName"_backup__view_materialize.sql
	rm -f $SQLDIR/"$relName"_backup__view_simple.sql
	rm -f $SQLDIR/"$relName"_backup__view_complex.sql
	rm -f $SQLDIR/"$relName"_backup__view_additional.sql
	rm -f $SQLDIR/"$relName"_backup_user_view_simple.sql
	rm -f $SQLDIR/"$relName"_backup_user_view_complex.sql
	rm -f $SQLDIR/"$relName"_backup_user_view_additional.sql
	rm -f $SQLDIR/"$relName"_backup_views_watchlist.dat
	
	cat $OUTDIR/"$relName"_backup_analysis_collect_results.out | grep -i "Found Backup View" | while read -r line ; do
	
		dbName=`echo $line | cut -f2 -d'|'`
		viewType=`echo $line | cut -f3 -d'|'`
		occurence=`echo $line | cut -f4 -d'|'`
		tabName=`echo $line | cut -f5 -d'|'`
		
		out_fileName=""
		
		case "$dbName" in
		
			"$prodView")
							case "$viewType" in
							"S")
								out_fileName="$relName"_backup__view_simple.sql
								;;
							"C")
								out_fileName="$relName"_backup__view_complex.sql
								;;
							"A")
								out_fileName="$relName"_backup__view_additional.sql
								;;
							esac
			;;
			"$prodUserView")
							case "$viewType" in
							"S")
								out_fileName="$relName"_backup_user_view_simple.sql
								;;
							"C")
								out_fileName="$relName"_backup_user_view_complex.sql
								;;
							"A")
								out_fileName="$relName"_backup_user_view_additional.sql
								;;
							esac
			;;
			"$prodTpfView")
							case "$viewType" in
							"S")
								out_fileName="$relName"_backup__view_simple.sql
								;;
							"C")
								out_fileName="$relName"_backup__view_complex.sql
								;;
							"A")
								out_fileName="$relName"_backup__view_additional.sql
								;;
							esac
			;;
			"$prodTpfUserView")
							case "$viewType" in
							"S")
								out_fileName="$relName"_backup_user_view_simple.sql
								;;
							"C")
								out_fileName="$relName"_backup_user_view_complex.sql
								;;
							"A")
								out_fileName="$relName"_backup_user_view_additional.sql
								;;
							esac
			;;
			"$prodMatView")
							out_fileName="$relName"_backup__view_materialize.sql
			;;
			"$prodKPBIView")
							out_fileName="$relName"_backup__view_materialize.sql
			;;
		esac
		
		if [ ! -z $out_fileName ]
		then
			create_backup_view "$dbName.$tabName" $SQLDIR/$out_fileName
			
			if [ $occurence -gt 2 ]
			then
				echo "$dbName.$tabName" >> $SQLDIR/"$relName"_backup_views_watchlist.dat
			fi
			
			echo ""$dbName"|"$tabName"" >> $AUDITDIR/"$relName"_original_view_count.dat
			echo ""$dbName"|UPGR_"$tabName"" >> $AUDITDIR/"$relName"_upgr_view_count.dat
		fi
		
	done
	
	
	echo "Completed Scripts for Backing up Views !! " >> $logFileName

	if [ $region == "NC" ] || [ $region == "SC" ]
	then
		ViewPattern="$prodMatView\\\.|$prodKPBIView\\\.|$prodView\\\.|$prodTpfView\\\."
		userViewPattern="$prodMatView\\\.|$prodKPBIView\\\.|$prodView\\\.|$prodUserView\\\.|$prodTpfView\\\.|$prodTpfUserView\\\."
	else
		ViewPattern="$prodMatView\\\.|$prodKPBIView\\\.|$prodView\\\."
		userViewPattern="$prodMatView\\\.|$prodKPBIView\\\.|$prodView\\\.|$prodUserView\\\."
	fi
	
	rm -f $SQLDIR/"$relName"_backup_script_creation_errors.sql
	
	if [ -f $SQLDIR/"$relName"_backup__view_materialize.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_materialize.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_materialize.sql
		cat $SQLDIR/"$relName"_backup__view_materialize.sql | egrep -i "$prodMatView\.|$prodKPBIView\." | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup__view_simple.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_simple.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_simple.sql
		cat $SQLDIR/"$relName"_backup__view_simple.sql | egrep -i "$ViewPattern" | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup__view_complex.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_complex.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_complex.sql
		cat $SQLDIR/"$relName"_backup__view_complex.sql | egrep -i "$ViewPattern" | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup__view_additional.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_additional.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_additional.sql
		cat $SQLDIR/"$relName"_backup__view_additional.sql | egrep -i "$ViewPattern" | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup_user_view_simple.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup_user_view_simple.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup_user_view_simple.sql
		cat $SQLDIR/"$relName"_backup_user_view_simple.sql | egrep -i "$userViewPattern" | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup_user_view_complex.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup_user_view_complex.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup_user_view_complex.sql
		cat $SQLDIR/"$relName"_backup_user_view_complex.sql | egrep -i "$userViewPattern" | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup_user_view_additional.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup_user_view_additional.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup_user_view_additional.sql
		cat $SQLDIR/"$relName"_backup_user_view_additional.sql | egrep -i "$userViewPattern" | grep -i -v "UPGR_"  >> $SQLDIR/"$relName"_backup_script_creation_errors.sql
	fi
	

	# Load all the original and backup names in a temp table for further analysis
	$SCRIPTDIR/epdba_runFastLoad.sh -h $TDPROD -o CLARITY_DBA_MAINT.UPG_BACKUP_REPLACE_LIST  -d $AUDITDIR/"$relName"_compare_backup_count.dat -l $logFileName 

	
	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Completed Generating Scripts for Backup -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	
