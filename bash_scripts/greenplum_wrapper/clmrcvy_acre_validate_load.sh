#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_acre_validate_load.sh ------------------------------------#


process_log_file()
{

remoteFile=$1
logFileName=$2
file_id=$3


        cd $TEMPDIR
        remoteFileLog="$remoteFile.*.log"
        user="clm_rcvy"


	echo "Starting Analysis on $remoteFileLog" >> $logFileName 

#STEP-4 Get Summary of total claims reported and update outpt_file table

        
        cat $remoteFileLog | grep  ' Total Loadable Records'  | \
             cut -f2 -d']' | cut -f1 -d':' > $TEMPDIR/acr_clm_rptd.tmp

        echo `sed 's/$/+/' $TEMPDIR/acr_clm_rptd.tmp` 0 | bc >> $logFileName
        total_clm_rptd=`tail -1 $logFileName`


#STEP-5  Generate the sql scripts to update outpt_file,trgt_sys_file and suspct_clm to REPORTED

        sql1="UPDATE $GPRPTSCHEMA.trgt_sys_file SET totl_clm_rptd=$total_clm_rptd, \
                 trgt_sys_file_stts='REPORTED', updtd_by=current_user, updt_dt=current_timestamp \
                 WHERE trgt_sys_file_id='$file_id' AND trgt_sys_nm='ACRE';"
	

        sql2="UPDATE $GPRPTSCHEMA.outpt_file SET outpt_file_stts='REPORTED', \
               updtd_by=current_user, updt_dt=current_timestamp \
               WHERE trgt_sys_file_id='$file_id' AND trgt_sys_nm='ACRE';"
       

	
        echo $sql1 >> $TEMPDIR/acre_validate_load.sql
        echo $sql2 >> $TEMPDIR/acre_validate_load.sql
        
	echo "SELECT outpt_file_id FROM $GPRPTSCHEMA.outpt_file WHERE trgt_sys_file_id=$file_id AND trgt_sys_nm='ACRE';" > $TEMPDIR/acre_find_outfile.sql

	if [ -f "$TEMPDIR/acre_find_outfile.dat" ]
	then
		rm -f $TEMPDIR/acre_find_outfile.dat
	fi


	$SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f "acre_find_outfile.sql" -l $TEMPDIR/acre_find_outfile.dat

	out_file_list=""
	cat $TEMPDIR/acre_find_outfile.dat | \
        while read nextFile
        do
                out_file_id=`echo $nextFile | sed  's/ //g'`
		
                if [ -n "$out_file_id" ] && [[ $out_file_id = +([0-9]) ]]
                then
		      out_file_list=`echo "$out_file_id,$out_file_list"`
                fi
        done

	sql3="UPDATE suspct_clm SET clm_stts_cd='REPORTED',    \
		updtd_by=current_user, updt_dt=current_timestamp  \
		WHERE outpt_file_id IN ($out_file_list); "

        echo $sql3 | sed  's/,)/)/g'  >> $TEMPDIR/acre_validate_load.sql

	sql4="UPDATE suspct_clm SET clm_stts_cd='REJECTED',    \
	             updtd_by=current_user, updt_dt=current_timestamp   \
	      WHERE suspct_clm_nbr IN (SELECT suspct_clm_nbr FROM acre_suspct_clm_reject \
			               WHERE acre_file_id=$file_id ); "

        echo $sql4 >> $TEMPDIR/acre_validate_load.sql


        cat $remoteFileLog | grep '(2)' | grep -v "Error - Record will not load" | \
            cut -f2 -d']' | cut -f3 -d' ' >  $TEMPDIR/acr_reject_count.tmp
        echo "Records Rejected : " | echo `sed 's/$/+/' $TEMPDIR/acr_reject_count.tmp` 0 | bc >> $logFileName


#STEP-5 Identify Summary Line for each error(get relative start position and claim count)

        cat $remoteFileLog | grep -n '(2)' | grep -v "Error - Record will not load" | \
             sed 's/ /|/g' | sed 's/:/|/g' | cut -f1,6,7-16 -d "|" > $TEMPDIR/acr_err_line_pos.tmp


#STEP-6 Grab each claim from start position(relative position + 4) till total count is reached

        
        cat $TEMPDIR/acr_err_line_pos.tmp | \
        while read LINE
        do
                clmStart=`echo $LINE | cut -f1 -d "|"` 
                clmCount=`echo $LINE | cut -f2 -d "|"`
                clmDesc=`echo $LINE | cut -d "|" -f 3-12 | sed 's/|/ /g'`
                currLine=`expr $clmStart + 4`

                if [ -f $TEMPDIR/acr_err_clm.tmp ]
                then
                        rm -f $TEMPDIR/acr_err_clm.tmp
                fi


        #STEP-7  Start writing each claim from start position till count is reached to a temp file
                i=0
                while [ $i -lt $clmCount ]
                do 
                        cat $remoteFileLog | nl | sed -n "$currLine p" | cut -f2 -d'[' | \
                        sed 's/]/ /g' | sed 's/\/FA/ FA/g' | tr -s ' ' | sed 's/ /|/g'  >> $TEMPDIR/acr_err_clm.tmp
                        currLine=`expr $currLine + 1`
                        i=`expr $i + 1`
                done

        #STEP-8 Format each record from temp file and move it to final file in '|' delimited format

                cat $TEMPDIR/acr_err_clm.tmp | \
                while read claimLine
                do
                        clmTime=`echo $claimLine | cut -d'|' -f 1-2 | sed 's/|/ /g'`
                        clmRecord=`echo $claimLine | cut -d'|' -f 3-10`
                        echo "$ACRENV|$remoteFile|$file_id|$clmRecord|$clmDesc|$clmTime:00|$user|$clmTime:00|$user"  >> $TEMPDIR/acre_suspct_clm_reject.dat
                done

        done


        echo "Completed Analysis on $remoteFileLog " >> $logFileName 

	return 0

}



#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#

#       ksh clmrcvy_acre_validate_load.sh -f 2001



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
                err_cd=901
                exit 901
        fi

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Started on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
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

        if [ $GP_ENV != "TEST" ] && [ $GP_ENV != "PROD" ]  && [ $GP_ENV != "DEV" ]
        then
                
                echo "ERROR - Invalid GP_ENV Value passed to run_sqlFile.sh  - $GP_ENV" >> $logFileName
                exit 911
        fi



# STEP-4 Initalize Data File and Query File.
#        Reset Value of  file_list

        if [ -f $TEMPDIR/acre_suspct_clm_reject.dat ]
        then
                rm -f $TEMPDIR/acre_suspct_clm_reject.dat 
        fi
        touch $TEMPDIR/acre_suspct_clm_reject.dat  

        cat $SQLDIR/acre_validate_load.sql > $TEMPDIR/acre_validate_load.sql

	file_list=""


# STEP-5 If File-Id is given generate the acr file for the file_id 
#        Else find each file in the table 


        if [ -n "$file_id" ] && [[ $file_id = +([0-9]) ]]
        then

                remoteFile=`psql -t -d gp_db -c "select TRIM(remote_file_nm) from $GPRPTSCHEMA.outpt_file WHERE outpt_file_id=$file_id"`

                if [ -z "$remoteFile" ]
                then
                        echo "remoteFile not found for $file_id" >> $logFileName
                        exit 902
                fi

                

                #STEP-4.1 FTP File passed as input
                $SCRIPTSDIR/clmrcvy_acre_get_log_file.sh $remoteFile $logFileName
        

                #STEP-4.2 Prcoess Each File
		remoteFileLog="$remoteFile.*.log"
                file_cnt=`ls -ltr $TEMPDIR/$remoteFileLog | wc -l | sed  's/ //g'`
                if [ "$file_cnt" != "0"  ]
                then
                        process_log_file $remoteFile $logFileName $file_id
                        rt_cd=$?
                        if  [ $rt_cd -eq 0 ]
                        then
                                file_list=`echo "$file_id,$file_list"`
                        fi
                else
                        echo "No Log Found for $remoteFile" >> $logFileName
                fi
        fi


	if [ -f "$TEMPDIR/acre_find_sent_files.dat" ]
	then
		rm -f $TEMPDIR/acre_find_sent_files.dat
	fi


        if [ -z "$file_id" ]
        then

	    $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f "acre_find_sent_files.sql" -l $TEMPDIR/acre_find_sent_files.dat

	    cat $TEMPDIR/acre_find_sent_files.dat | \
            while read nextLine
            do

                nextLine=`echo $nextLine | sed  's/ //g'`
                file_id=`echo $nextLine | cut -f1 -d';'`
                remoteFile=`echo $nextLine | cut -f2 -d';'`

                if [ -n "$file_id" ] && [[ $file_id = +([0-9]) ]]
                then
                        # For each file that is in sent status - 
                        # read the log from acre server and analyze results

                        echo "Fetch Acre Log for $remoteFile" >> $logFileName 
                        
                        #STEP-4.1 FTP File passed as input
                        $CODE/clmrcvy_gp/scripts/clmrcvy_acre_get_log_file.sh $remoteFile $logFileName
                
		        rt_cd=$?
                        if  [ $rt_cd -eq 0 ]
                        then
                                #STEP-4.2 Prcoess Each File
                                remoteFileLog="$remoteFile.*.log"
                                file_cnt=`ls -ltr $TEMPDIR/$remoteFileLog | wc -l | sed  's/ //g'`
                                if [ "$file_cnt" != "0"  ]
                                then
                                        process_log_file $remoteFile $logFileName $file_id
                                        rt_cd=$?
                                        if  [ $rt_cd -eq 0 ]
                                        then
                                                file_list=`echo "$file_id,$file_list"`
                                        fi

                                        # Move the file to Target Files to avoid double counting
                                        mv $TEMPDIR/$remoteFileLog $TGTFILES/clmrcvy
                                else
                                        echo "No Log Found for $remoteFile" >> $logFileName
                                fi
                        fi
                fi
            done
        fi

                        
# STEP-6 Call Script perform gpload

        chmod 775 $TEMPDIR/acre_suspct_clm_reject.dat

        echo "Started GPLOAD at `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1


	$SCRIPTSDIR/run_gpLoad.sh -s $TEMPDIR/acre_suspct_clm_reject.dat -t clm_rcvy.acre_suspct_clm_reject \
	                        -e clm_rcvy_utlty.acre_gpload_err -y acre_suspct_clm_reject.yml  \
				-j CRFAC_RESP_ACRE_0001 -f 0 -l $logFileName

	rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "GPLOAD Utility Failed for acre_suspct_clm_reject.yml with status code $rt_cd" >> $logFileName
                err_cd=909
                exit 909
        fi

        echo "Ended GPLOAD at `date '+%m/%d/%Y %T'`" >> $logFileName


        if [ -f $TEMPDIR/acre_suspct_clm_reject.dat ]
        then
                rm -f $TEMPDIR/acre_suspct_clm_reject.dat
        fi


# STEP-7 Update the Status in suspct_clm and outpt_file table

        $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'acre_validate_load.sql' -l $logFileName   
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




# STEP-8 If file_list is not null then generate load summary report 

	if [ "$file_list" != "" ]
        then
                
                  report="select A.trgt_sys_file_id,A.trgt_sys_file_desc,    \
                          SUBSTRING(A.trgt_sys_file_wrksht_nm,1,2) acre_err_type,  \
                          SUBSTRING(A.trgt_sys_file_wrksht_nm,4,2) acre_err_id,    \
                          A.trgt_sys_file_nm, A.trgt_file_sent_on,     \
                          A.totl_clm_sent,A.totl_clm_amt_sent,A.totl_clm_rptd totl_clm_loaded,    \
                          (A.totl_clm_amt_sent-COALESCE(B.totl_rej_clm_amt,0)) totl_clm_amt_loaded  \
                        from $GPRPTSCHEMA.trgt_sys_file A   \
                        LEFT JOIN $GPRPTSCHEMA.trgt_sys_file_reject B ON A.trgt_sys_file_id=B.trgt_sys_file_id   \
                        where A.trgt_sys_file_id IN ( $file_list)   \
                        ORDER BY A.trgt_sys_file_id;"

                load_date=`date +"%Y_%m_%d"`

                cd $TEMPDIR
                echo $report | sed  's/,)/)/g' > rpt_acr_validation.sql

                psql -h $GPHOST -U $GPUSER -d $GPDB -A -F ',' -f rpt_acr_validation.sql > rpt_acr_validation.csv

               # tr '|' ',' < rpt_acr_validation.dat > rpt_acr_validation.csv

                uuencode rpt_acr_validation.csv ACR_$GP_ENV_FILES_LOADED_$load_date.csv | \
                mailx -s "Central Facets File(s) Loaded in ACR $GP_ENV. See Load summary attached to this mail for details." $ACRNOTIFY
         
	fi


        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Ended on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1



#------------------------ End of Script clmrcvy_acre_validate_load.sh ---------------------------------------#



