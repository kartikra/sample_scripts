#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_acre_send_file.sh ------------------------------------#


# STEP-1 Read Input Parameters

	localFile=$1
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


	echo "Parameters for FTP transfer are .. "   >> $logFileName
	echo "ACRE HOST : $ACRHOST"  >> $logFileName
	echo "ACRE USER : $ACRUSER"  >> $logFileName
	echo "ACRE DIR  : $ACRDIR"  >> $logFileName
	echo "LOCAL DIR : $SRCFILES"  >> $logFileName


	echo "Start FTP for $localFile .. "   >> $logFileName


# STEP-3 Get Log File from ACRE

        cd $SRCFILES

        ftp -n $ACRHOST << INPUT_END >> $logFileName
        quote USER $ACRUSER
        quote PASS $ACRPWD
        cd $ACRDIR
        prompt off
        put $localFile
        quit
        INPUT_END 



#---------------------- End of Script clmrcvy_acre_send_file.sh ------------------------------------#
