#!/usr/bin/ksh
#---------------------------------- Start of Script clmrcvy_cs90_drg_readm.sh -----------------------------------------------#
#######################################################################################################################
#  Filename:  clmrcvy_cs90_drg_readm.sh
#
#  Category:  Landing Zone to Staging (lztostg)
#
#  Description:
#              This shell script is a wrapper module for CS90 DRG Readmit Queries.
#              script take the following parameters as input -
#              1) list of input requirement ids (between 1 to 7)
#              2) A recovery reference dates which can be current, future or past date.
#                 All recovery timeframe computations will be with reference  to this date.
#              3) list of input source system codes for CS90
#              The script has 3 steps at a high level -
#              Run Analysis Query, Apply Exclusions,Run Reporting Query
#
#              Each analysis query is scheduled in the background. This enables the sql queries
#              to run in parallel. Once all the analysis queries are complete,
#              the exclusions are applied
#
#  PARAMETER(s):
#               [ rqtList ]     :- List of DRG Readmit Requirements       MANDATORY
#               [ srcList ]     :- List of Source Codes                 MANDATORY
#
#
#  Othe Shell(s) Invoked from this shell:
#
#                 sqlFile.sh    :- Processes sqlFile and records results in a logile.
#                                  Calling shell must pass sqlFile Directory, sqlFile Name
#                                  and logFile Name
#
#  Main Processing:
#
#     Step 1.   Run the .profile file for clm_rcvy. Abort script if profile file does not run successfully
#
#     Step 2.   Get all the input Parameters using getopts.
#
#     Step 3.   Create the log file for current run. Abort the script if the logfile is not created.
#
#     Step 4.   Validate the Input Parameters. Fail program if any parameter is missing
#
#     Step 5.   Run the Hew Request queries for all 3 source systems
#
#     Step 7.   Apply all the Exclusions
#
#     Step 8.   Run the Hew reporting Query
#
#     Step 9. Check for errors in reporting query. Abort script if any errors are encountered in the log file
#
#     Step 10. If every STEP above completes succesfully, end the program with a return code of 0
#
#
#  Return Code:   If Return Code is not equal to 0
#                 check the log file for execptions encountered during processing
#
#  Log File Name:  Log File created under - (always look at log with latest timestamp)
#                  /export/home/clm_rcvy/logs/clmrcvy_hew_request_all.sh_<curr_ts>.log
#
#
#--------------------------------------------------------------------------------------------------
#          Examples for Invoking Shell  :-
#                   ksh clmrcvy_hew_request_all.sh -s 809 -r 1,4,7  -n y -a y
#                   ksh clmrcvy_hew_request_all.sh -r 1,7 -s 809 -n y -a y
#                   ksh clmrcvy_hew_request_all.sh -r 1 -s 809 -n n -a n
#                   ksh clmrcvy_hew_request_all.sh -r 1,7 -s 809 -a y -n y
#######################################################################################################################
#---------------------------------------GLOBAL VARIABLES----------------------------------------------------------#

#-------------------------------------- BEGIN  FUNCTIONS ---------------------------------------------------------#

report_status ()
{
        while getopts a:n:s: par
        do      case "$par" in
                a)      attachFile="$OPTARG";;
                n)      notifyInd="$OPTARG";;
                s)      jobStatus="$OPTARG";;
                [?])    echo "Correct Usage -->  report_status -a <attachFile> -s <jobStatus> -n <notifyInd>"
                        exit 998;;
                esac
        done




        #------------------------------------------------------------------------------------------------------#

        start_dt=`head -3 $LOGDIR/$attachFile | tail -1 | cut -f7 -d' '`
        start_tm=`head -3 $LOGDIR/$attachFile | tail -1 | cut -f8 -d' '`

        echo "$start_dt || $start_tm" >> $logFileName
	echo "'$start_dt $start_tm'"  >> $logFileName

        fieldList="run_cntrl_id,job_cd,job_typ_cd,strt_tm,end_tm,run_stts, \
                   creatn_dt,creatd_by,updt_dt,updtd_by,updt_host, \
                   run_desc,log_file_nm"

       runDesc="HEW REQUEST ALL $GP_ENV RUN"

       fieldValueList="nextval('seq_run_cntrl_id'),'HEW_RQST_ALL_001','99', \
                        '$start_dt $start_tm',current_timestamp,'$jobStatus',current_timestamp, \
                        current_user,current_timestamp,user,'BATCH', \
                        '$runDesc','$attachFile'"

       echo "INSERT INTO run_cntrl($fieldList) VALUES($fieldValueList)" > $SQLDIR/run_hew_rqst_all.sql
       
       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f run_hew_rqst_all.sql -l $logFileName
       rt_cd=$?
       if [ $rt_cd -ne 0 ]
       then
              echo "ERROR - Unable to Run SQL File" >> $logFileName
              exit 101
       fi

       rm -f $SQLDIR/run_hew_rqst_all.sql

        #------------------------------------------------------------------------------------------------------#


        if [ $notifyInd == 'y' ]
        then
                echo 'Sending Notification Email'  >> $logFileName
        $SCRIPTSDIR/send_mail.sh -s $jobStatus  -d "HEW REQUEST ALL $GP_ENV" -a $attachFile -t cs90_dev
#               -b noncob_bus -m mgmt
        fi
}



run_hew_cs90()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_hew_cs90 -l <logFileName> -s<srcList>"
                        report_status  -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        

 sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_cs90_hew_request.sql > $TEMPDIR/clmrcvy_cs90_hew_request.sql
        rt_cd1=$?
 
        if [ $rt_cd1 -eq 0 ] 
        then
	echo 'Running Analysis Queries For CS90 Hew request in background'  >> $logFileName
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_cs90_hew_request.sql' -l $logFileName
	# Check for errors in HCFA staging queries
	
	      err_cnt=`grep "ERROR" $logFileName | wc -l`
	      echo 'Error Count is $err_cnt' >> $logFileName
	       if [ $err_cnt -ne 0 ]
		 then
	              echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
	              report_status  -a $attachFile -n $notifyInd -s 'FAILURE 102'
		 exit 102
	       fi

        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status  -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}

run_hew_wgs()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_hew_wgs -l <logFileName> -s<srcList>"
                        report_status  -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        

 sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_wgsstar_hew_request.sql > $TEMPDIR/clmrcvy_wgsstar_hew_request.sql
        rt_cd1=$?
 
        if [ $rt_cd1 -eq 0 ] 
        then
	echo 'Running Analysis Queries For WGSSTAR Hew request in background'  >> $logFileName
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_wgsstar_hew_request.sql' -l $logFileName
	# Check for errors in HCFA staging queries
	
	      err_cnt=`grep "ERROR" $logFileName | wc -l`
	      echo 'Error Count is $err_cnt' >> $logFileName
	       if [ $err_cnt -ne 0 ]
		 then
	              echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
	              report_status  -a $attachFile -n $notifyInd -s 'FAILURE 102'
		 exit 102
	       fi

        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status  -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}

run_hew_facets()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_hew_facets -l <logFileName> -s<srcList>"
                        report_status  -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        

 sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_hew_request.sql > $TEMPDIR/clmrcvy_facets_hew_request.sql
        rt_cd1=$?
 
        if [ $rt_cd1 -eq 0 ] 
        then
	echo 'Running Analysis Queries For Facets Hew request in background'  >> $logFileName
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_hew_request.sql' -l $logFileName
	# Check for errors in HCFA staging queries
	
	      err_cnt=`grep "ERROR" $logFileName | wc -l`
	      echo 'Error Count is $err_cnt' >> $logFileName
	       if [ $err_cnt -ne 0 ]
		 then
	              echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
	              report_status  -a $attachFile -n $notifyInd -s 'FAILURE 102'
		 exit 102
	       fi

        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status  -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}


#--------------------------------------- END  FUNCTIONS ---------------------------------------------------------#

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#


# STEP-1 Run the .profile file for clm_rcvy to set all GP_ENVironment variables


        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file cannot be found, Exiting"
                exit 902
        fi

# STEP-2 Read Input Parameters

        while getopts r:s:n:a: par
        do      case "$par" in
                r)      rqtList="$OPTARG";;
                s)      srcList="$OPTARG";;
                n)      notifyInd="$OPTARG";;
                a)      acrInd="$OPTARG";;
                [?])    echo "Correct Usage -->  clmrcvy_hew_request_all.sh -r <rqtList> -s <srcList> -a <acrInd> -n <notify>"
                        exit 999;;
                esac
        done



##STEP-3 Create Log File for current run

        today=`date +"%Y_%m_%d_%H_%M_%S"`
        scriptName=`basename $0`

        logFileName="$LOGDIR/$scriptName-$today.log"
        attachFile="$scriptName-$today.log"

        echo "START" >> $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Create LogFile"
                exit 901
        fi


        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Started in $GP_ENV at: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "           Requirement(s) -  $rqtList " >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1
              
#STEP-4 Validate Input Parameters

        if [ $GP_ENV != "DEV" ] && [ $GP_ENV != "TEST" ] && [ $GP_ENV != "PROD" ]
        then

                echo "ERROR - Invalid GP_ENV Value - $GP_ENV" >> $logFileName
                exit 911
        fi


        if [ -z "$srcList" ]
        then
                echo "ERROR - Missing Required Parameter - srcList " >> $logFileName
                echo "Correct Usage --> clmrcvy_hew_request_all.sh -r <rqtList> -d <recovRefDate> -s <srcList>"  >> $logFileName
                report_status  -a $attachFile -n $notifyInd -s 'FAILURE 201'
                exit 201
        fi

        if [ -z "$notifyInd" ]
        then
                notifyInd='n'
        else
                if [ $notifyInd != 'y' ] && [ $notifyInd != 'n' ]
                then
                        echo "ERROR - Invalid Value  $notifyInd for notifyInd. Valid Values are y and n " >> $logFileName
                        report_status  -a $attachFile -n 'n' -s 'FAILURE 201'
                        exit 201
                fi
        fi

        if [ -z "$acrInd" ]
        then
                acrInd='n'
        else
                if [ $acrInd != 'y' ] && [ $acrInd != 'n' ]
                then
                        echo "ERROR - Invalid Value  $acrInd for acrInd. Valid Values are y and n " >> $logFileName
                        report_status  -a $attachFile -n $notifyInd -s 'FAILURE 201'
                        exit 201
                fi
        fi


#STEP-5a Run the DRG Readmit query for each requirement after cleaning up results of last run

       if [ ! -z "$rqtList" ]
       then

               # Clear the results from the previous run
               #echo 'Cleaning up results from last run' >> $logFileName
               #$SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_hew_request_cs90.sql' -l $logFileName
               #rt_cd=$?
               #if [ $rt_cd -ne 0 ]
               #then
               #        echo "ERROR - Unable to cleanup results of last run" >> $logFileName
               #        report_status  -a $attachFile -n $notifyInd -s 'FAILURE 101'
               #        exit 101
               #fi


               # Get first requirement from the list

               tmp_rqtList=$rqtList
               rqt=`echo $tmp_rqtList | cut -f1 -d,"`
               while [ ! -z "$tmp_rqtList" ]
               do
                       # Evaluate the requerement id and call the corresponding module
                       case "$rqt" in
                               1) run_hew_cs90 -l $logFileName -s $srcList;;

                               2) run_hew_wgs -l $logFileName -s $srcList;;

                               3) run_hew_facets -l $logFileName -s $srcList;;

                               *)
                                       echo "ERROR - Requirement $rqt not defined" >> $logFileName
                                       report_status  -a $attachFile -n $notifyInd -s 'FAILURE 204'
                                       exit 204
                               ;;
                       esac


                       # Logic to get the next requirment from the list
                       rqtRepl=$rqt
                       tmp_rqtList=`echo $tmp_rqtList | tr -d $rqtRepl`
                       if [ ! -z "$rqtList" ]
                       then
                               tmp_rqtList=`echo $tmp_rqtList | sed 's/,/ /'`
                               rqt=`echo $tmp_rqtList | cut -f1 -d,"`
                       fi
               done
       else
               echo "ERROR - No Requirements Specified" >> $logFileName
               exit 203
       fi


# STEP-11  If all STEPS above complete succesfully, end the program with a return code of 0

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Ended Normally in $GP_ENV at: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1

        report_status  -a $attachFile -n $notifyInd -s 'SUCCESS'


#------------------------------------------ END MAIN SCRIPT ---------------------------------------------#


