#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_sql_load.sh ------------------------------------#

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#
#CODE="/gpfs01/dev/edl/code"
#PMDIR="/gpfs01/dev/edl/pcenterdata"

#STEP-1 Read Parameters

sql_file=$1
jobCode=$2
notifyInd=$3


runDesc="run_$sql_file"


#STEP-2 Run Profile File

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi
	
        
#STEP-3 Create Log File for current run 

	today=`date +"%Y_%m_%d_%H_%M_%S"`
        shellName=`basename $0`
        logFileName=$LOGDIR/$shellName-$today.log
	touch $logFileName
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then
		echo "ERROR - Unable to Create LogFile"
		exit 901
	fi

	chmod 775 $logFileName


	echo "**************************************************************************************" >> $logFileName 2>&1
	echo "Program $shellName Started on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
	echo "**************************************************************************************" >> $logFileName 2>&1


	if [ -z "$jobCode" ]
        then
                jobCode="NA"
        fi


#STEP-4 Create Entry for current run on start of job

        
       fieldList="run_cntrl_id,job_cd,job_typ_cd,strt_tm,end_tm,run_stts, \
		   creatn_dt,creatd_by,updt_dt,updtd_by,updt_host, \
		   run_desc,log_file_nm, infa_run_id"
		   
       fieldValueList="nextval('seq_run_cntrl_id'),'$jobCode',4, \
			current_timestamp,current_timestamp,'STARTED',current_timestamp, \
			current_user,current_timestamp,user,'BATCH', \
			'$sql_file','$logFileName', 0"

       echo "INSERT INTO run_cntrl($fieldList) VALUES($fieldValueList);" > $SQLDIR/$runDesc

       sql_command="SELECT MAX(run_cntrl_id) FROM run_cntrl \
             WHERE job_cd='$jobCode'  AND run_stts='STARTED' \
                  AND log_file_nm='$logFileName';" 

       echo $sql_command >> $SQLDIR/$runDesc

       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f $runDesc -l $logFileName  
       rt_cd=$?
       if [ $rt_cd -ne 0 ]
       then
	      echo "ERROR - Unable to Run SQL File" >> $logFileName
	      exit 101
       fi

       rm -f $SQLDIR/$runDesc

       run_ctrl_id=`tail -3 $logFileName | head -1 | sed 's/ //g'`
       echo $run_ctrl_id >> $logFileName


#STEP-5  Run the queries in the sql file

	if [  -f "$SQLDIR/$sql_file" ]
        then
               
		sed -e 's/var_run_id/'$run_ctrl_id'/g' $SQLDIR/$sql_file > $TEMPDIR/$sql_file

                echo "Started $sql_file at `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f $sql_file -l $logFileName
                rt_cd=$?
                if [ $rt_cd -ne 0 ]
                then
                        echo "ERROR - $rt_cd : Unable to Run sql in file $final_target.sql" >> $logFileName
                        exit 815
                fi

                echo "Ended $sql_file at `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
		
		sql_err_cnt=`cat $logFileName | tail -5 | grep "ERROR" | wc -l`
		if [ $sql_err_cnt -ne 0 ]
		then
			echo "ERROR - while running $sql_file"  >> $logFileName
                        exit 815
		fi

        else
               echo "SQL File $sql_file Not Found"  >> $logFileName 2>&1
	       exit 104
        fi



#STEP-6 Update Status of current run

        sql_command="UPDATE run_cntrl SET \
                trgt_insrtd_rows=0, \
                trgt_updtd_rows=0, \
                trgt_deleted_rows=0, \
                run_stts='SUCCESS',   \
                end_tm=current_timestamp,updtd_by=current_user, \
                updt_dt=current_timestamp \
                WHERE job_cd='$jobCode'  AND run_stts='STARTED' \
                  AND log_file_nm='$logFileName' " 
	
	echo $sql_command > $SQLDIR/$runDesc
	

	$SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f $runDesc -l $logFileName  
	rt_cd=$?
	if [ $rt_cd -ne 0 ]
	then
	       echo "ERROR - Unable to Run SQL File" >> $logFileName
	       exit 101
	fi

	rm -f $SQLDIR/$runDesc



# STEP-7 Send Notifcation Email if notify ind = y

	echo "**************************************************************************************" >> $logFileName 2>&1
	echo "Program $scriptName Ended on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
	echo "**************************************************************************************" >> $logFileName 2>&1


	if [ "$notifyInd" == "Y" ]
	then
		`sed 's/$//g' $logFileName > $TEMPDIR/mail.dat`
		uuencode $TEMPDIR/mail.dat $shellName-$today.log |  \
		mailx -s "JOB $jobCode : Data Refresh Completed for SQL File $sql_file !! " \
		"dl-BABW-Claims-Accuracy-Tech@wellpoint.com " 
	fi
