#!/usr/bin/ksh


# STEP-1 Read Input Parameters

	TDHOST=$1
	sqlFile=$2
	outFile=$3
	flagFile=$4



	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi



# STEP-2 Run the user profile file and set user credentials

	USR_PROF=$HOME/user.profile
        . $USR_PROF >> $logFile 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Error while running runSQLFile.sh - user.profile cannot be found, Exiting" >> $logFile
                exit 902
        fi


	case "$TDHOST" in
   		"tdp1.didi.com") REPO=$RECP1
   		;;
   		"tdp2.didi.com") REPO=$RECP2
   		;;
   		"tdp3.didi.com") REPO=$RECP3
   		;;
		"tdp5.didi.com") REPO=$RECP5
   		;;
   		"tdd1.didi.com") REPO=$RECD1
   		;;
   		"tdd3.didi.com") REPO=$RECD3
   		;;
		"tdd4.didi.com") REPO=$RECD4
   		;;
	esac

	
	
# STEP-3 Execute Bteq Script

rm -f $outFile
bteq <<EOI
.logon $TDHOST/$USER,$REPO;
.SET WIDTH 15000;
.SET ERROROUT STDOUT
.EXPORT REPORT FILE = $outFile; 
.RUN FILE = $sqlFile
.EXPORT RESET; 
.LOGOFF;
.EXIT;
EOI
	if [  ! -z "$flagFile"  ]
	then
		touch $flagFile
	fi



