#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_acre_create_output_file.sh ------------------------------------#

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#



# STEP-1 Run the .profile file for clm_rcvy to set all environment variables

	USR_PROF=$HOME/.profile
	. $USR_PROF > /dev/null 2>&1
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then  
		echo "Profile .profile cannot be found, Exiting"
		exit 903
	fi
	USR_PROF=$HOME/clmrcvy.profile
	. $USR_PROF > /dev/null 2>&1
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then  
		echo "Profile clmrcvy.profile cannot be found, Exiting"
		exit 903
	fi


#STEP-2 Create Log File for current run
	
	scriptName=`basename $0`
	today=`date +"%Y_%m_%d_%H_%M_%S"`
	logFileName=$LOGDIR/$scriptName-$today.log

	touch $logFileName
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then
		echo "ERROR - Unable to Create LogFile"
		err_cd=901
		exit 901
	fi

	echo "**************************************************************************************" >> $logFileName 2>&1
	echo "Program $scriptName Started on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
	echo "**************************************************************************************" >> $logFileName 2>&1


	acre_seqNo=`psql -t -c "SELECT nextval('clm_rcvy.seq_acre_submsn_no')"`
	acre_seqNo=`echo $acre_seqNo  | sed 's/ //g'`
	localFileName="$ACRUSER _Log $acre_seqNo _00.man"
	localFileName=`echo $localFileName | sed 's/ //g'`
	localFile=$EXPORTACRE/$localFileName

	sed -e 's/var_file_name/'$localFileName'/g' $SQLDIR/create_acr_file.sql > $TEMPDIR/create_acr_file.sql

	$SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'acre_create_output_file.sql' -l $logFileName   
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then
		echo "SQL in $sqlFileName completed abruptly with code - $rt_cd" >>  $logFileName
		exit 908
	else
		echo "SQL in $sqlFileName completed normally with code - $rt_cd" >>  $logFileName
	fi
        err_cnt=`grep -i "ERROR:" $logFileName | wc -l`
	if [ $err_cnt -ne 0 ]
	then
		echo "ERROR - while running $sqlFileName" >> $logFileName
		exit 102
	fi


	psql -t -A -F '' -E -c "SELECT * FROM $GPSTGSCHEMA.acr_report_final_stg" 1> $localFile 2>> $logFileName
	if [ $rt_cd -ne 0 ]
	then
		echo "Command completed abruptly with code - $rt_cd" >>  $logFileName
		exit 908
	else
		echo "Command completed normally with code - $rt_cd" >>  $logFileName
	fi

        echo "Start FTP for $localFile" >> $logFileName 


#STEP-3 FTP File passed as input

	cd $EXPORTACRE

        ftp -n $ACRHOST << INPUT_END >> $logFileName
        quote USER $ACRUSER
        quote PASS $ACRPWD
        cd $ACRDIR
        prompt off
        put $localFileName
        quit
        INPUT_END  


        echo "**************************************************************************************" >> $logFileName 2>&1
	echo "Program $scriptName Ended on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
	echo "**************************************************************************************" >> $logFileName 2>&1


#------------------------ End of Script clmrcvy_acre_create_output_file.sh ---------------------------------------#