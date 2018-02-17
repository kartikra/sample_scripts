#!/usr/bin/ksh

# STEP-1 Read Input Parameters


 	while getopts s:t:f:u: par
        do      case "$par" in
                s)      TDSRC="$OPTARG";;
                t)      TDTGT="$OPTARG";;
                f)      MIGRATIONFILE="$OPTARG";;
                u)      utiltiy_db="$OPTARG";;

                [?])    echo "Correct Usage -->  ksh epdba_migrate_data.sh -s <td source> -t <td target> -u <utiltiy_db> -f <input list file> "
                        exit 998;;
                esac
        done



# STEP-2 Run the user profile file and set user credentials

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi

		scriptName=`basename $0`
		dateforlog=`date +%Y%m%d%H%M%S`
		logName=$scriptName-${dateforlog}.log
		logFileName=$LOGDIR/$logName

		
		
# STEP-3 Copy Data from Source to Target		
		
		cat $MIGRATIONFILE | while read -r line ; do

			SRCTABLE=`echo $line | cut -f1 -d'|'`
			TGTTABLE=`echo $line | cut -f2 -d'|'`
		
			fileName="$SRCTABLE".dat
		
			$SCRIPTDIR/epdba_runFastExport.sh -h $TDSRC -i $SRCTABLE  -d $IMPEXP/$fileName -l $logFileName 
			$SCRIPTDIR/epdba_runFastLoad.sh -h $TDTGT -d $IMPEXP/$fileName -o $TGTTABLE  -l $logFileName

		done
		

		