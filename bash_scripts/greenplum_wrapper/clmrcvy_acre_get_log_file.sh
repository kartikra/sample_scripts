#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_acre_get_log_file.sh ------------------------------------#


# STEP-1 Read Input Parameters

	remoteFile=$1
	logFileName=$2

	

#STEP-2 Run Profile File

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi

	. $LOGON/clmrcvy_acre_logon.ctrl > /dev/null 2>&1
	rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Logon file clmrcvy_acre_logon.ctrl cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi

	
	if [ $GP_ENV != "TEST" ] && [ $GP_ENV != "PROD" ] && [ $GP_ENV != "DEV" ]
	then		
		echo "ERROR - Invalid ENV Value - $GP_ENV" >> $logFileName
		exit 911
	fi


	remoteFileLog=`echo "$remoteFile.*.log"`

	echo "Parameters for FTP transfer are .. "   >> $logFileName
	echo "ACRE HOST : $ACRHOST"  >> $logFileName
	echo "ACRE USER : $ACRUSER"  >> $logFileName
	echo "ACRE DIR  : $ACRDIR"  >> $logFileName
	echo "LOCAL DIR : $TEMPDIR"  >> $logFileName

	echo "Start FTP for $remoteFileLog" >> $logFileName 
	

# STEP-3 Get Log File from ACRE

        cd $TEMPDIR

        ftp -n $ACRHOST << INPUT_END >> $logFileName
        quote USER $ACRUSER
        quote PASS $ACRPWD
        cd $ACRLOG
        prompt off
        mget $remoteFileLog
        quit
        INPUT_END 

        echo "End FTP for $remoteFileLog" >> $logFileName 


#---------------------- End of Script clmrcvy_acre_get_log_file.sh ------------------------------------#
