#!/usr/bin/ksh

# Before Starting Replace HCCLPGA2 and tdp2 with required values

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

	ticketNo="WO0000003083281"
	databaseName="HCCLPGA2"
	TDPROD="tdp2.kp.org"

	#rm -f $DDLBACKUPDIR/export_"$databaseName"_ddl.sql
	# #$SCRIPTDIR/epdba_export_ddl.sh -h $TDPROD -d $databaseName -o export_"$databaseName"_ddl.sql

	rm -f $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql
	sed -e 's/'[Rr][Ee][Pp][Ll][Aa][Cc][Ee]\ *'/'REPLACE\ '/g' \
	-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ *'/'REPLACE\ '/g' \
	-e 's/'[Cc][Vv]\ *'/'REPLACE\ VIEW\ '/g' \
	-e 's/'[Cc][Rr][Ee][Aa][Tt][Ee]\ '/'REPLACE\ '/g' -e 's/'[Cc][Vv]\ '/'REPLACE\ VIEW\ '/g' \
	$DDLBACKUPDIR/export_"$databaseName"_ddl.sql > $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql
	
	
	if [ -f $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql ]
	then
		$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $DDLBACKUPDIR modified_"$databaseName"_ddl.sql
		perl -pi -e 's/\bREPLACE\ VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql
	fi
	
	rm -f $TEMPDIR/java_results.out
	cd $JAVADIR
	java -DoutputFile="$TEMPDIR/java_results.out" -DinputFile="$DDLBACKUPDIR/modified_"$databaseName"_ddl.sql" RemoveLockTable
	rm -f $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql
	mv $TEMPDIR/java_results.out $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql
	
	#$SCRIPTDIR/epdba_runSQLFile.sh $TDPROD $DDLBACKUPDIR/modified_"$databaseName"_ddl.sql $OUTDIR/modified_"$databaseName"_ddl.out | tee -a $logFileName

