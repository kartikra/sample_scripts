#!/usr/bin/ksh
#----------------------------- Start of Script clmrcvy_acre_submit_load.sh -----------------------------------------------#


# ksh clmrcvy_acre_submit_load.sh -e TEST

create_acre_file ()
{

        file_id=$1

        #fileNm=`echo $ACRUSER | sed 's/_//g' `

        localFileName="acreftp_Log $file_id _00.man"
        localFileName=`echo $localFileName | sed 's/ //g'`
        localFile=$TEMPDIR/$localFileName

        sed -e 's/var_file_id/'$file_id'/g' $SQLDIR/acre_create_load_file.sql > $TEMPDIR/acre_create_load_file_$file_id.sql


        $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f "acre_create_load_file_$file_id.sql" -l $logFileName 
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


        sed -e 's/var_schema/'$GPSTGSCHEMA'/g' $SQLDIR/acre_send_final_file.sql > $TEMPDIR/acre_send_final_file.sql

        psql -h $GPHOST -U $GPUSER -d $GPDB -t -A -F '|' -E -f $TEMPDIR/acre_send_final_file.sql 1> $localFile 2>> $logFileName
        if [ $rt_cd -ne 0 ]
        then
                echo "Command completed abruptly with code - $rt_cd" >>  $logFileName
                exit 908
        else
                echo "Command completed normally with code - $rt_cd" >>  $logFileName
        fi

        
        tr -d '|' < $localFile > $SRCFILES/$localFileName


        #----------------------  Start of Send File to ACRE    --------------------------#

        $SCRIPTSDIR/clmrcvy_acre_send_file.sh $localFileName $logFileName

        err_cnt=`grep -i "ERROR:" $logFileName | wc -l`
        if [ $err_cnt -ne 0 ]
        then
                echo "ERROR - while sending $localFileName" >> $logFileName
                exit 102
        fi

        echo "End FTP for $localFile"   >> $logFileName

        #-----------------------  End of Send File to ACRE    ----------------------------#


        mv $SRCFILES/$localFileName $SRCFILES/clmrcvy/$localFileName

        command=" UPDATE $GPRPTSCHEMA.trgt_sys_file \
                  SET trgt_sys_file_nm='$localFileName', \
                      trgt_sys_host_nm='$ACRENV',  \
                      trgt_sys_file_lctn='$ACRDIR',  \
                      trgt_file_sent_on=current_timestamp, \
                      trgt_sys_file_stts='SENT'  \
                  WHERE trgt_sys_file_id=$file_id;"


        echo $command > $TEMPDIR/acre_create_load_file_$file_id.sql

        command=" UPDATE $GPRPTSCHEMA.outpt_file  \
                 SET outpt_file_nm='$localFileName',  \
                     outpt_file_sent_on=current_timestamp,  \
                     outpt_file_stts='SENT'   \
                WHERE trgt_sys_file_id=$file_id  \
                   AND outpt_file_stts='GENERATED';"

       echo $command >> $TEMPDIR/acre_create_load_file_$file_id.sql



       command=" SELECT outpt_file_id FROM $GPRPTSCHEMA.outpt_file 
                 WHERE trgt_sys_file_id=$file_id;"

        echo $command >> $TEMPDIR/acre_find_out_files.sql
        $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f "acre_find_out_files.sql" -l $TEMPDIR/acre_find_out_files.dat

        cat $TEMPDIR/acre_find_out_files.dat | \
        while read nextFile
        do
                out_file_id=`echo $nextFile | sed  's/ //g'`
                if [ -n "$out_file_id" ] && [[ $out_file_id = +([0-9]) ]]
                then

                        command=" UPDATE $GPRPTSCHEMA.suspct_clm \
                        SET clm_stts_cd='SENT'  \
                        WHERE outpt_file_id=$out_file_id  \
                          AND clm_stts_cd='GENERATED';"

                        echo $command > $TEMPDIR/acre_create_load_file_$file_id.sql
                fi
        done


        $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f "acre_create_load_file_$file_id.sql" -l $logFileName 
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "SQL in $sqlFileName completed abruptly with code - $rt_cd" >>  $logFileName
                exit 908
        else
                echo "SQL in $sqlFileName completed normally with code - $rt_cd" >>  $logFileName
        fi


        mv $TEMPDIR/$localFileName $SRCFILES
        rm -f $TEMPDIR/acre_create_load_file_$file_id.sql


        return 0

}

        

# STEP-1 Run the .profile file for clm_rcvy to set all environment variables

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile clmrcvy.profile cannot be found, Exiting"
                exit 903
        fi


        . $LOGON/clmrcvy_acre_logon.ctrl > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Logon file clmrcvy_acre_logon.ctrl cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
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
                exit 901
        fi

        chmod 775 $logFileName

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Started on $GP_ENV : `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1

        

# STEP-3 Read and Validate Input Parameters

        while getopts f: par
        do      case "$par" in
                f)      file_id="$OPTARG";;
                [?])    echo "Correct Usage -->  clmrcvy_submit_load.sh -f <fileId> " 
                        err_cd=999
                        exit 999;;
                esac
        done

        if [ $GP_ENV != "TEST" ] && [ $GP_ENV != "PROD" ] && [ $GP_ENV != "DEV" ]
        then
                
                echo "ERROR - Invalid GP_ENV Value passed to run_sqlFile.sh  - $GP_ENV" >> $logFileName
                exit 911
        fi

      
# STEP-4 If File-Id is given generate the acr file for the file_id 
#        Else find each file in the table 


        if [ -n "$file_id" ]
        then
                create_acre_file $file_id 
                err_cd=$?
                exit $err_cd
        fi


        if [ -f $TEMPDIR/acre_find_gen_files.dat ]
        then
                rm -f $TEMPDIR/acre_find_gen_files.dat
        fi

        $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f "acre_find_gen_files.sql" -l $TEMPDIR/acre_find_gen_files.dat

        cat $TEMPDIR/acre_find_gen_files.dat | \
        while read nextFile
        do
                file_id=`echo $nextFile | cut -f1 -d'|' | sed  's/ //g'`
                wks_nm=`echo $nextFile | cut -f2 -d'|' | sed  's/ //g'`
                sent_tm=`echo $nextFile | cut -f3 -d'|'`

                if [ -n "$file_id" ] && [[ $file_id = +([0-9]) ]]
                then

                        command=" UPDATE outpt_file \
                        SET trgt_sys_file_id=$file_id  \
                        WHERE outpt_file_wrksht_nm='$wks_nm'  \
                          AND outpt_file_stts='GENERATED'  \
                          AND trgt_sys_nm='ACRE'   \
                          AND outpt_file_sent_on - TIMESTAMP '$sent_tm' < INTERVAL'1 minute' ;"

                        echo $command > $TEMPDIR/acre_update_outfile.sql

                        $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f "acre_update_outfile.sql" -l $logFileName

                        create_acre_file $file_id 
                fi
        done


        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Ended on $GP_ENV : `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1
#
#       `sed 's/$//g' $logFileName > $TEMPDIR\acre_mail.dat`
#       uuencode $TEMPDIR\acre_mail.dat $scriptName-$today.log |  \
#       mailx -s "Central Facets File(s) SENT to ACR $GP_ENV. See Load summary attached to this mail for details." $ACRNOTIFY

#------------------------ End of Script clmrcvy_acre_submit_load.sh ---------------------------------------#
       
err_cd=0
