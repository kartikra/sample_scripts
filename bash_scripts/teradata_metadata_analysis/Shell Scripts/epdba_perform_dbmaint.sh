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
	echo "--------------- Preparing DB Maintenace Job -------------------" >> $logFileName
	echo "---------------------- Option : $1 for $2 --------------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName

	
	option=$1	# 1=Drop Table or 2=Delete Data
	dbName=$2	# Database Name that needs cleanup
	TDRegion=$3	# Teradata Region
	ticketNo=$4	# Ticket Number
	listFile=$5

	rm -f $TEMPDIR/"$dbName"_get_list_db_tables.out
	rm -f $TEMPDIR/"$dbName"_list_db_table_maintenace.sql
	touch $TEMPDIR/"$dbName"_list_db_table_maintenace.sql


# STEP-3 Start DB Maintenance - Generate Table List if not provided

	if [ -z "$listFile" ]
	then
		if [ "$option" == "1" ]
		then
			cat $SQLDIR/accdba_drop_alltables_ddl.sql | sed -e 's/$$DBNAME/'$dbName'/g' | sed -e 's/$$TICKET/'$ticketNo'/g'  > $TEMPDIR/"$dbName"_get_list_db_tables.sql
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDRegion $TEMPDIR/"$dbName"_get_list_db_tables.sql $TEMPDIR/"$dbName"_get_list_db_tables.out | tee -a $logFileName
		fi
	
		if [ "$option" == "2" ]
		then
			echo "SELECT '\"' || TRIM(DatabaseName) || '\".\"' || TRIM(TableName) || '\"' from DBC.TablesV WHERE TableKind='T' AND DatabaseName LIKE '$dbName'" > $TEMPDIR/"$dbName"_get_list_db_tables.sql 
			$SCRIPTDIR/epdba_runSQLFile2.sh $TDRegion $TEMPDIR/"$dbName"_get_list_db_tables.sql $TEMPDIR/"$dbName"_get_list_db_tables.out | tee -a $logFileName
		fi
		
		sed '1d' $TEMPDIR/"$dbName"_get_list_db_tables.out > $TEMPDIR/"$dbName"_get_list_db_tables.tmp
		mv $TEMPDIR/"$dbName"_get_list_db_tables.tmp $TEMPDIR/"$dbName"_get_list_db_tables.out 
		
	else
		cat $listFile > $TEMPDIR/"$dbName"_get_list_db_tables.out
	fi



# STEP-4 Start DB Maintenance - Loop through the list created above

	cat $TEMPDIR/"$dbName"_get_list_db_tables.out | while read -r line ; do
	
		if [ "$option" == "1" ]
		then
			echo "$line "  >> $TEMPDIR/"$dbName"_list_db_table_maintenace.sql
		fi

		if [ "$option" == "2" ]
		then
			echo "DELETE FROM /*$ticketNo*/ $line ALL;" >> $TEMPDIR/"$dbName"_list_db_table_maintenace.sql
		fi

	done


# STEP-5 Perform DB Maintenace if valid ticket has been provided

	if [ -z "$ticketNo" ]
	then
		echo "Sorry Cannot Run the script without valid ticket !!" >> $logFileName
	else
		#$SCRIPTDIR/epdba_runSQLFile.sh $TDRegion $TEMPDIR/"$dbName"_list_db_table_maintenace.sql $TEMPDIR/"$dbName"_get_list_db_tables.out | tee -a  $logFileName
		$SCRIPTDIR/epdba_runMultipleSQLFile.sh $TDRegion 14 $TEMPDIR "$dbName"_list_db_table_maintenace.sql $TEMPDIR/"$dbName"_list_db_table_maintenace.out $logFileName 	
	fi

	rm -f $TEMPDIR/"$dbName"_*.*

	#touch $FLAGDIR/$scriptName-${dateforlog}.flag


	echo "---------------------------------------------------------------" >> $logFileName
	echo "--------------- Completed DB Maintenace Job -------------------" >> $logFileName
	echo "---------------------------------------------------------------" >> $logFileName
