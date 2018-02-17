#!/usr/bin/ksh
#---------------------------------- Start of Script clmrcvy_facets_dup.sh -----------------------------------------------#
#######################################################################################################################
#  Filename:  clmrcvy_facets_dup.sh
#
#  Category:  Landing Zone to Staging (lztostg)
#
#  Description:
#              This shell script is a wrapper module for CRDW Duplicate Queries.
#              script take the following parameters as input -
#              1) list of input requirement ids (between 1 to 7)
#              2) A recovery reference dates which can be current, future or past date.
#                 All recovery timeframe computations will be with reference  to this date.
#              3) list of input source system codes for CRDW
#              The script has 3 steps at a high level -
#              Run Analysis Query, Apply Exclusions,Run Reporting Query
#
#              Each analysis query is scheduled in the background. This enables the sql queries
#              to run in parallel. Once all the analysis queries are complete,
#              the exclusions are applied
#
# psql -U  " + context.gpDbUserName + " -d " + context.gpUnixScriptDir + "cor_check_detail_lz.yml -l cor_check_detail_lz.log"
#  PARAMETER(s):
#               [ rqtList ]     :- List of Duplicate Requirements       MANDATORY
#               [ recovRefDate] :- Reference Date for Computing
#                                  Recovery timelines                   MANDATORY
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
#     Step 5.   Run the duplicate analysis queries for each requirement.
#               Conitnue to loop through for each requirement passed in the input
#               Call the corresponding procedure for each requirement after cleaning up results
#               of the previous run.
#
#     Step 6.   Check if analysis queries for all requirements are complete
#               Wait till all analysis queries are complete. Conitnue checking for completion
#               after every 5 seconds. Abort script if any errors are encountered in the log file
#
#     -----------------------------------------------------------------------------------------------
#       IMPORTANT POINTS to Keep in mind for Step-5 and Step-6
#       -----------------------------------------------------------
#       1:- All sql file names must begin with clmrcvy_facets_dup for the sed logic to work
#
#       2:- Call run_sqlFile.sh to run each sqlFile. While calling this shell pass
#                the sql file dir, sql file name and log file name as input
#                This shell will log both results and errors in the log file specified
#
#       3:- Do not introduce any comments in the logfile with the word "ERROR".
#    -----------------------------------------------------------------------------------------------
#
#     Step 7.  Apply Header and Line Level Exclusions
#
#     Step 8.  Get Sum of claim lines for line level initatives
#
#     Step 9.   Run the duplicate reporting Query
#               Replace var_date parameter in the sql file with the date passed from input
#
#     Step 10. Check for errors in reporting query. Abort script if
#              any errors are encountered in the log file
#
#     Step 11. If every STEP above completes succesffully, end the program with a retrun code of 0
#
#
#
#  Function : run_exact_duplicate
#
#     Step 1.   Get all the input Parameters using getopts.
#
#     Step 2.   Run query for UB Header in background.
#               Replace var_source parameter in the sql file with the sourceCode passed from input
#
#     Step 3.   Run query for UB and HCFA line in background.
#               Replace var_source parameter in the sql file with the sourceCode passed from input
#
#
#
#  Final Output:  Records will be inserted into the following tables -
#                 clm_rcvy_stg.crdw_dup_hdr_stg
#                 clm_rcvy_stg.crdw_dup_line_stg
#                 clm_rcvy_stg.dup_sum_clm_stg
#                 clm_rcvy_stg.dup_line_exclsn_stg
#                 clm_rcvy_stg.crdw_dup_hdr_final_stg
#                 clm_rcvy_stg.crdw_dup_line_final_stg
#
#  Return Code:   If Return Code is not equal to 0
#                 check the log file for execptions encountered during processing
#
#  Log File Name:  Log File created under - (always look at log with latest timestamp)
#
#
#
#--------------------------------------------------------------------------------------------------
#          Examples for Invoking Shell  :-
#                   sh clmrcvy_facets_dup.sh -s 167 -r 1,2,3,4,7  -n y -a y
#                   sh clmrcvy_facets_dup.sh -r 1,2,3,4,7 -s 167 -n y -a y
#                   sh clmrcvy_facets_dup.sh -r 1,2,3,4,7 -s 167 -n y -a y
#                   sh clmrcvy_facets_dup.sh -r 1,2,3,4,7 -s 167 -a y -n y
#######################################################################################################################
#---------------------------------------GLOBAL VARIABLES----------------------------------------------------------#

#-------------------------------------- BEGIN  FUNCTIONS ---------------------------------------------------------#

report_status()
{
        while getopts a:n:s: par
        do      case "$par" in
                a)      attachFile="$OPTARG";;
                n)      notifyInd="$OPTARG";;
                s)      jobStatus="$OPTARG";;
                [?])    echo "Correct Usage -->  report_status -a <attachFile> -s<jobStatus> -n<notifyInd>"
                        exit 998;;
                esac
        done

        #------------------------------------------------------------------------------------------------------#

        start_dt=`head -2 $LOGDIR/$attachFile | tail -1 | cut -f7 -d' '`
        start_tm=`head -2 $LOGDIR/$attachFile | tail -1 | cut -f8 -d' '`

        fieldList="run_cntrl_id,job_cd,job_typ_cd,strt_tm,end_tm,run_stts, \
                   creatn_dt,creatd_by,updt_dt,updtd_by,updt_host, \
                   run_desc,log_file_nm"

       runDesc="CR FACETS DUPLICATE $GP_ENV RUN"

       fieldValueList="nextval('seq_run_cntrl_id'),'CRFAC_QRY_DUP_0001','11', \
                        '$start_dt $start_tm',current_timestamp,'$jobStatus',current_timestamp, \
                        current_user,current_timestamp,user,'BATCH', \
                        '$runDesc','$attachFile'"

       echo "INSERT INTO run_cntrl($fieldList) VALUES($fieldValueList)" > $SQLDIR/run_dup_crdw.sql


       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f run_dup_crdw.sql -l $logFileName
       rt_cd=$?
       if [ $rt_cd -ne 0 ]
       then
              echo "ERROR - Unable to Run SQL File" >> $logFileName
              exit 101
       fi

       rm -f $SQLDIR/run_dup_crdw.sql

       #------------------------------------------------------------------------------------------------------#


        if [ $notifyInd == 'y' ]
        then
                echo 'Sending Notification Email'  >> $logFileName
        $SCRIPTSDIR/send_mail.sh -s $jobStatus  -d "CR FACETS DUPLICATES $GP_ENV" -a $attachFile -t facets_dev -b noncob_bus -m mgmt
        fi
}


run_exact_duplicate()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_exact_duplicate -l <logFileName> -s<srcList>"
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For Exact Duplicate in background'  >> $logFileName

# Run UB Exact Duplicate at header level
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_exact_ub_hdr.sql > $TEMPDIR/clmrcvy_facets_dup_exact_ub_hdr.sql
        rt_cd=$?
        if [ $rt_cd -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_exact_ub_hdr.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi

# Run UB and HCFA Exact Duplicate at line Level
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_exact_hcfa_line.sql > $TEMPDIR/clmrcvy_facets_dup_exact_hcfa_line.sql
        rt_cd1=$?
      sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_exact_ub_line.sql > $TEMPDIR/clmrcvy_facets_dup_exact_ub_line.sql
        rt_cd2=$?
        if [ $rt_cd1 -eq 0 ] && [ $rt_cd2 -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_exact_hcfa_line.sql' -l $logFileName &
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_exact_ub_line.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}

run_different_billed_amount()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_exact_duplicate -l <logFileName> -s<srcList>"
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For Diffrent Billed in background'  >> $logFileName

# Run UB Diffrent Billed at header level
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_diff_billed_ub_hdr.sql > $TEMPDIR/clmrcvy_facets_dup_diff_billed_ub_hdr.sql
        rt_cd=$?
        if [ $rt_cd -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_diff_billed_ub_hdr.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi

# Run UB and HCFA Diffrent Billed at line Level
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_diff_billed_hcfa_line.sql > $TEMPDIR/clmrcvy_facets_dup_diff_billed_hcfa_line.sql
        rt_cd1=$?
      sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_diff_billed_ub_line.sql > $TEMPDIR/clmrcvy_facets_dup_diff_billed_ub_line.sql
        rt_cd2=$?
        if [ $rt_cd1 -eq 0 ] && [ $rt_cd2 -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_diff_billed_hcfa_line.sql' -l $logFileName &
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_diff_billed_ub_line.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}



run_Overlapping()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage --> run_exact_duplicate -l <logFileName> -s<srcList>"
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For Overlapping'  >> $logFileName


# Run UB and HCFA Overlapping at line Level
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_over_lap_hcfa_line.sql > $TEMPDIR/clmrcvy_facets_dup_over_lap_hcfa_line.sql
        rt_cd1=$?
     sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_over_lap_ub_line.sql > $TEMPDIR/clmrcvy_facets_dup_over_lap_ub_line.sql
        rt_cd2=$?
        if [ $rt_cd1 -eq 0 ] && [ $rt_cd2 -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_over_lap_hcfa_line.sql' -l $logFileName &
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_over_lap_ub_line.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}

run_different_prov_amount()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_exact_duplicate -l <logFileName> -s<srcList>"
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For Different provider in background'  >> $logFileName

# Run UB Different provider at header level
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_diff_prov_ub_hdr.sql > $TEMPDIR/clmrcvy_facets_dup_diff_prov_ub_hdr.sql
        rt_cd=$?
        if [ $rt_cd -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_diff_prov_ub_hdr.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi

# Run UB and HCFA Different provider at line Level
        sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_diff_prov_hcfa_line.sql > $TEMPDIR/clmrcvy_facets_dup_diff_prov_hcfa_line.sql
        rt_cd1=$?
       sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_diff_prov_ub_line.sql > $TEMPDIR/clmrcvy_facets_dup_diff_prov_ub_line.sql
        rt_cd2=$?
        if [ $rt_cd1 -eq 0 ] && [ $rt_cd2 -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_diff_prov_hcfa_line.sql' -l $logFileName &
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_diff_prov_ub_line.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi
}


run_interim_bill()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_exact_duplicate -l <logFileName> -s<srcList>"
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For Interim Bill in background'  >> $logFileName



# Run UB Interim Bill Claim at header level
        sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_interim_ub_hdr.sql > $TEMPDIR/clmrcvy_facets_dup_interim_ub_hdr.sql
        rt_cd=$?
        if [ $rt_cd -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_interim_ub_hdr.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi

}


run_asc()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_exact_duplicate -l <logFileName> -s<srcList>"
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For asc in background'  >> $logFileName



# Runs asc Claim at line level
        sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_facets_dup_asc_line.sql > $TEMPDIR/clmrcvy_facets_dup_asc_line.sql
        rt_cd=$?
        if [ $rt_cd -eq 0 ]
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_asc_line.sql' -l $logFileName &
        else
                echo "ERROR - Unable to Substitute parameters" >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 202'
                exit 202
        fi

}


#--------------------------------------- END  FUNCTIONS ---------------------------------------------------------#



#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#


# STEP-1 Run the .profile file for clm_rcvy to set all environment variables

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
                [?])    echo "Correct Usage -->  clmrcvy_facets_dup.sh -r <rqtList> -s <srcList> -a <acrInd> -n <notify>"
                        exit 999;;
                esac
        done




##STEP-3 Create Log File for current run

        today=`date +"%Y_%m_%d_%H_%M_%S"`
        scriptName=`basename $0`

        logFileName="$LOGDIR/$scriptName-$today.log"
        attachFile="$scriptName-$today.log"
        echo "START" $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Create LogFile"
                exit 901
        fi
        chmod 775 $logFileName

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Started in $ENV at: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "           Requirement(s) -  $rqtList " >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1


#STEP-4 Validate Input Parameters

        if [ "$GP_ENV" != "DEV" ] && [ "$GP_ENV" != "TEST" ] && [ "$GP_ENV" != "PROD" ]
        then

                echo "ERROR - Invalid ENV Value - $GP_ENV" >> $logFileName
                exit 911
        fi


        if [ -z "$srcList" ]
        then
                echo "ERROR - Missing Required Parameter - srcList " >> $logFileName
                echo "Correct Usage -->  clmrcvy_facets_dup.sh -r <rqtList> -d <recovRefDate> -s <srcList>"  >> $logFileName
                report_status -a $attachFile -n $notifyInd -s 'FAILURE 201'
                exit 201
        fi
        if [ -z "$notifyInd" ]
        then
                notifyInd='n'
        else
                if [ $notifyInd != 'y' ] && [ $notifyInd != 'n' ]
                then
                        echo "ERROR - Invalid Value  $notifyInd for notifyInd. Valid Values are y and n " >> $logFileName
                        report_status -a $attachFile -n 'n' -s 'FAILURE 201'
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
                        report_status -a $attachFile -n $notifyInd -s 'FAILURE 201'
                        exit 201
                fi
        fi

#STEP-5 Run the duplicate query for each requirement after cleaning up results of last run

       if [ ! -z "$rqtList" ]
       then

               # Clear the results from the previous run
               echo 'Cleaning up results from last run' >> $logFileName
               $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_facets_dup_cleanup.sql' -l $logFileName
               rt_cd=$?
               if [ $rt_cd -ne 0 ]
               then
                       echo "ERROR - Unable to cleanup results of last run" >> $logFileName
                       report_status -a $attachFile -n $notifyInd -s 'FAILURE 101'
                       exit 101
               fi


               # Get first requirement from the list

               tmp_rqtList=$rqtList
               rqt=`echo $tmp_rqtList | cut -f1 -d,"`
               while [ ! -z "$tmp_rqtList" ]
               do
                       # Evaluate the requerement id and call the corresponding module
                       case "$rqt" in
                               1) run_exact_duplicate -l $logFileName -s $srcList;;
                               2) run_different_billed_amount -l $logFileName -s $srcList;;
                               3) run_different_prov_amount -l $logFileName -s $srcList;;
                               4) run_Overlapping -l $logFileName -s $srcList;;
                               6) run_asc -l $logFileName -s $srcList;; 
                               7) run_interim_bill -l $logFileName -s $srcList;;

                               *)
                                       echo "ERROR - Requirement $rqt not defined" >> $logFileName
                                       report_status -a $attachFile -n $notifyInd -s 'FAILURE 204'
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


       sleep 5

#STEP-6a Check if analysis queries for all requirements are complete

       echo 'Analysis queries for all requirements submitted in background'  >> $logFileName
       row_cnt=`ps -ef | grep clmrcvy_facets_dup | wc -l`
       while [ $row_cnt -gt 2 ]
       do
               sleep 5
               row_cnt=`ps -ef | grep clmrcvy_facets_dup | wc -l`
       done


#STEP-6b Check for errors in analysis queries
       err_cnt=`grep -i "ERROR" $logFileName | wc -l`
       if [ $err_cnt -ne 0 ]
       then
               echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
               report_status -a $attachFile -n $notifyInd -s 'FAILURE 102'
               exit 102
       fi
       echo 'Analysis queries for all requirements completed successfully'  >> $logFileName


#STEP-7 Apply Header and Line Level Exclusions

       echo 'Starting Header and Line Exclusions'  >> $logFileName
       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_facets_dup_hdr_exclsn.sql' -l $logFileName
       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_facets_dup_line_exclsn.sql' -l $logFileName


#STEP-8 Check for errors in exclusion queries
       err_cnt=`grep -i "ERROR" $logFileName | wc -l`
       if [ $err_cnt -ne 0 ]
       then
               echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
               report_status -a $attachFile -n $notifyInd -s 'FAILURE 102'
               exit 102
       fi
       echo 'Queries for applying exclusions completed successfully'  >> $logFileName



#STEP-9 Get Sum of claim lines for line level initatives

       echo 'Starting Aggregation of Claim Lines'  >> $logFileName
       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_facets_dup_sum_lines.sql' -l $logFileName
       err_cnt=`grep -i "ERROR" $logFileName | wc -l`
       if [ $err_cnt -ne 0 ]
       then
               echo "ERROR - while running clmrcvy_facets_dup_sum_lines.sql"  >> $logFileName
               report_status -a $attachFile -n $notifyInd -s 'FAILURE 101'
               exit 101
       fi
       echo 'Completed aggregation of all duplicate claims at line level'  >> $logFileName


#STEP-10 Report All Requirements for Duplicates

       echo "Starting Reporting Query with ref date of $recovRefDate"  >> $logFileName
      sed -e 's/var_date/'$recovRefDate'/g' $SQLDIR/clmrcvy_facets_dup_rpt_claims.sql > $TEMPDIR/clmrcvy_facets_dup_rpt_claims.sql
       $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_facets_dup_rpt_claims.sql' -l $logFileName


#STEP-11 Check for errors in reporting query

       err_cnt=`grep -i "ERROR" $logFileName | wc -l`
       if [ $err_cnt -ne 0 ]
       then
               echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
               report_status -a $attachFile -n $notifyInd -s 'FAILURE 102'
               exit 102
       fi
       echo 'Reporting queries for all requirements completed successfully'  >> $logFileName

        if [ "$acrInd" == 'y' ]
        then
                echo 'Preparing to load Suspect Claim Table .. '  >> $logFileName

                $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_facets_dup_suspct_clm.sql' -l $logFileName
                rt_cd=$?
                if [ $rt_cd -eq 0 ]
                then
                        echo 'Suspect Claim Table Loaded successfully'  >> $logFileName
                else
                        exit $rt_cd
                fi



        fi


#STEP-14  If all STEPS above complete succesffully, end the program with a retrun code of 0

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Ended Normally in $GP_ENV at: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1

        report_status -a $attachFile -n $notifyInd -s 'SUCCESS'


#------------------------------------------ END MAIN SCRIPT ---------------------------------------------#



