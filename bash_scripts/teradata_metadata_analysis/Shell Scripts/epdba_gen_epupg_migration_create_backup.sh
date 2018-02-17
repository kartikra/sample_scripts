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
	
	
	runId="4"
	relName="ga0626"
	ticketNo="CRQ000000211731"
    regionProfile="REGNGAM"
	
	region="GA"
	newComment="/*$ticketNo*/"
	prefix=`echo $relName | awk '{print substr($0,3,4)}'`
	
	
	USR_PROF=$HOMEDIR/region/$regionProfile.profile
	. $USR_PROF > /dev/null 2>&1
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then
			echo "Profile file $regionProfile.profile cannot be found, Exiting"
			exit 902
	fi


create_backup_view () {	
		
	in_view=$1
	out_file=$2
	
	echo "SHOW VIEW $in_view;" > $TEMPDIR/get_view.sql
	$SCRIPTDIR/epdba_runSQLFile2.sh $TDPROD $TEMPDIR/get_view.sql $TEMPDIR/get_view.out | tee -a  $logFileName


	
	cp $TEMPDIR/get_view.out $TEMPDIR/get_view.out

	perl -pi -e 's/'CREATE\ *VIEW'/'REPLACE\ VIEW'/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'CV\ *HCCL'/'REPLACE\ VIEW\ HCCL'/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'CV\ *\"*HCCL'/'REPLACE\ VIEW\ \"HCCL'/gi'  $TEMPDIR/get_view.out 
	
	
	grep -i -n  "$prodUserView" $TEMPDIR/get_view.out > $TEMPDIR/get_cols.tmp
	grep -i -n  "$prodView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
	grep -i -n  "$prodMatView" $TEMPDIR/get_view.out >> $TEMPDIR/get_cols.tmp
	
	cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
				sed  ''$lineNumber's/\"//g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
				mv $TEMPDIR/get_view.tmp $TEMPDIR/get_view.out
	done
	
	perl -pi -e 's/'$prodView'\./'$prodView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'$prodUserView'\./'$prodUserView'\.UPGR_/gi'  $TEMPDIR/get_view.out 
	perl -pi -e 's/'$prodMatView'\./'$prodMatView'\.UPGR_/gi'  $TEMPDIR/get_view.out 

	cat $SQLDIR/archive/$relName/backupdb_replace.list | while read -r chgList ; do
		findWord=`echo $chgList | cut -f1 -d '|'`
		replaceWord=`echo $chgList | cut -f2 -d '|'`
		perl -pi -e 's/'$findWord'/'$replaceWord'/gi'  $TEMPDIR/get_view.out 
	done
	
	# Replace any space after UPGR_
	sed  's/UPGR\_\ /UPGR\_/g'  $TEMPDIR/get_view.out > $TEMPDIR/get_view.tmp
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
		#exit 902
	fi
	
	$SCRIPTDIR/epdba_runSQLFile.sh "$TDPROD" $SQLDIR/accdba_backup_analysis_collect_results.sql $OUTDIR/"$relName"_backup_analysis_collect_results.out | tee -a  $logFileName

	
	#--------------------------- Scripts for Backing Up DDL of all impacted databases ------------------------

	#$SCRIPTDIR/epdba_export_ddl.sh -e $TDPROD -l $SQLDIR/archive/$relName/backup_ddl.list &
	#echo "Running DDL Backup Scripts in Background ...." >> $logFileName

	
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

	
	
	echo "Audit Files have been created !! " >> $logFileName

	
	#--------------------------- Create Scripts for Backing Up Tables ------------------------
	echo "Creating Scripts for Backing up Tables ...." >> $logFileName

	rm -f $SQLDIR/"$relName"_backup_create_tables.sql
	rm -f $SQLDIR/"$relName"_backup_delete_data_from_backup_tables.sql
	rm -f $SQLDIR/"$relName"_backup_enable_blocklevel_compression.sql
	rm -f $SQLDIR/"$relName"_backup_migrate_data_from_prod_tables.sql

	echo "SET QUERY_BAND='BlockCompression=YES;' FOR SESSION;" > $SQLDIR/"$relName"_backup_migrate_data_from_prod_tables.sql
	
	cat $OUTDIR/"$relName"_backup_analysis_collect_results.out | grep -i "Found Backup Table" | while read -r line ; do
	
		dbName=`echo $line | cut -f2 -d'|'`
		tabName=`echo $line | cut -f3 -d'|'`
	
		backupDB=`cat $SQLDIR/archive/$relName/backupdb_replace.list | grep -i $dbName | head -1 | cut -f2 -d'|'`
	
		if [ -z "backupDB" ] 
		then
			echo "Backp Database Not found in replacement list for $dbName"
		else
			
			echo "CREATE TABLE /*$ticketNo*/ $backupDB.\"$tabName\" AS $dbName.\"$tabName\" WITH DATA AND STATS;" >> $SQLDIR/"$relName"_backup_create_tables.sql
			
			echo "DELETE FROM /*$ticketNo*/ $backupDB.\"$tabName\" ALL;" >> $SQLDIR/"$relName"_backup_delete_data_from_backup_tables.sql
			
			echo "ALTER TABLE /*$ticketNo*/ $backupDB.\"$tabName\",NO FALLBACK, BLOCKCOMPRESSION = MANUAL;" >> $SQLDIR/"$relName"_backup_enable_blocklevel_compression.sql

			echo "INSERT INTO /*$ticketNo*/ $backupDB.\"$tabName\" SELECT \\* FROM $dbName.\"$tabName\";" >> $SQLDIR/"$relName"_backup_migrate_data_from_prod_tables.sql

			echo ""$dbName"|"$tabName"" >> $AUDITDIR/"$relName"_source_table_count.dat
			echo ""$backupDB"|"$tabName"" >> $AUDITDIR/"$relName"_backup_table_count.dat
			
			
		fi
	
	done
	
	echo "Completed Scripts for Backing up Tables !! " >> $logFileName


	
	
	#--------------------------- Create Scripts for Backing Up Views ------------------------
	echo "Creating Scripts for Backing up Views ...." >> $logFileName

	
	rm -f $SQLDIR/"$relName"_backup__view_materialize.sql
	rm -f $SQLDIR/"$relName"_backup__view_simple.sql
	rm -f $SQLDIR/"$relName"_backup__view_complex.sql
	rm -f $SQLDIR/"$relName"_backup_user_view_simple.sql
	rm -f $SQLDIR/"$relName"_backup_user_view_complex.sql
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

		
	if [ -f $SQLDIR/"$relName"_backup__view_materialize.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_materialize.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_materialize.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup__view_simple.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_simple.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_simple.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup__view_complex.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_complex.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_complex.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup__view_additional.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup__view_additional.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup__view_additional.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup_user_view_simple.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup_user_view_simple.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup_user_view_simple.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup_user_view_complex.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup_user_view_complex.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup_user_view_complex.sql
	fi
	if [ -f $SQLDIR/"$relName"_backup_user_view_additional.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR "$relName"_backup_user_view_additional.sql
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/"$relName"_backup_user_view_additional.sql
	fi
	
	
	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Completed Generating Scripts for Backup -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
	
