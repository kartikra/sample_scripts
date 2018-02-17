#!/usr/bin/ksh
#---------------------------------- Start of Script clmrcvy_crfacets_hew_comp.sh--------------------------------------------#

#--------------------------------------------------------------------------------------------------
#          Examples for Invoking Shell  :-
#                   ksh clmrcvy_crfacets_hew_comp.sh-n y -a y
#                   ksh clmrcvy_crfacets_hew_comp.sh-n n -a n
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

        fieldList="run_cntrl_id,job_cd,job_typ_cd,strt_tm,end_tm,run_stts, \
                   creatn_dt,creatd_by,updt_dt,updtd_by,updt_host, \
                   run_desc,log_file_nm"

       runDesc="CRFACETS HEW Comparison $GP_ENV RUN"

       fieldValueList="nextval('clm_rcvy.seq_run_cntrl_id'),'FACETS_QRY_HEW_COMP','11', \
                        '$start_dt $start_tm',current_timestamp,'$jobStatus',current_timestamp, \
                        current_user,current_timestamp,user,'BATCH', \
                        '$runDesc','$attachFile'"

       echo "INSERT INTO run_cntrl($fieldList) VALUES($fieldValueList)" > $SQLDIR/run_hew_comp_crfacets.sql


       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f run_hew_comp_crfacets.sql -l $logFileName
       rt_cd=$?
       if [ $rt_cd -ne 0 ]
       then
              echo "ERROR - Unable to Run SQL File" >> $logFileName
              exit 101
       fi

       rm -f $SQLDIR/run_hew_comp_crfacets.sql

        #------------------------------------------------------------------------------------------------------#


        if [ $notifyInd == 'y' ]
        then
                echo 'Sending Notification Email'  >> $logFileName
        $SCRIPTSDIR/send_mail.sh -s $jobStatus  -d "CRFACETS HEW Comp $GP_ENV" -a $attachFile -t cs90_dev
#               -b noncob_bus -m mgmt
        fi
}


run_hew_comparison()
{
        while getopts l:s: par
        do      case "$par" in
                l)      logFileName="$OPTARG";;
                s)      srcList="$OPTARG";;
                [?])    echo "Correct Usage -->  run_hew_comparison -l <logFileName> -s<srcList>"
                        report_status  -a $attachFile -n $notifyInd -s 'FAILURE 998'
                        exit 998;;
                esac
        done

        echo 'Running Analysis Queries For Hew Comparison'  >> $logFileName

 sed -e 's/var_source/'$srcList'/g' $SQLDIR/clmrcvy_crfacets_hew_comparison.sql > $TEMPDIR/clmrcvy_crfacets_hew_comparison.sql

        rt_cd1=$?
 
        if [ $rt_cd1 -eq 0 ] 
        then
                $SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_crfacets_hew_comparison.sql' -l $logFileName

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
                [?])    echo "Correct Usage -->  clmrcvy_crfacets_hew_comp.sh -r <rqtList> -s <srcList> -a <acrInd> -n <notify>"
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
                echo "Correct Usage --> clmrcvy_crfacets_hew_comp.sh -r <rqtList> -d <recovRefDate> -s <srcList>"  >> $logFileName
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


#STEP-5a Run the HEW Comparison query for each requirement after cleaning up results of last run

       if [ ! -z "$rqtList" ]
       then

               # Clear the results from the previous run
#               echo 'Cleaning up results from last run' >> $logFileName
#               $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_crfacets_hew_comp_cleanup.sql' -l $logFileName
#               rt_cd=$?
#               if [ $rt_cd -ne 0 ]
#               then
#                       echo "ERROR - Unable to cleanup results of last run" >> $logFileName
#                       report_status  -a $attachFile -n $notifyInd -s 'FAILURE 101'
#                       exit 101
#               fi


               # Get first requirement from the list

               tmp_rqtList=$rqtList
               rqt=`echo $tmp_rqtList | cut -f1 -d,"`
               while [ ! -z "$tmp_rqtList" ]
               do
                       # Evaluate the requerement id and call the corresponding module
                       case "$rqt" in
                               1) run_hew_comparison -l $logFileName -s $srcList;;

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
#STEP-6a Check if analysis queries for all requirements are complete

       echo 'Analysis queries for all requirements submitted'  >> $logFileName
       row_cnt=`ps -ef | grep clmrcvy_crfacets_hew_comp | wc -l`
       while [ $row_cnt -gt 2 ]
       do
               sleep 5
               row_cnt=`ps -ef | grep clmrcvy_crfacets_hew_comp | wc -l`
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


#STEP-7 Apply  Exclusions for HEW Comparison

#       echo 'Starting HEW Comparison Exclusions'  >> $logFileName
#       $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_crfacets_hew_comparison_exclsn.sql' -l $logFileName

#STEP-8 Check for errors in exclusion queries
#       err_cnt=`grep -i "ERROR" $logFileName | wc -l`
#       if [ $err_cnt -ne 0 ]
#       then
#               echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
#               report_status  -a $attachFile -n $notifyInd -s 'FAILURE 102'
#               exit 102
#       fi
#       echo 'Queries for applying exclusions completed successfully'  >> $logFileName


#STEP-9 Report All Requirements for HEW Comparison

#echo "Starting Reporting Query with ref date of $recovRefDate"  >> $logFileName
#sed -e 's/var_date/'$recovRefDate'/g' $SQLDIR/clmrcvy_crfacets_cob_hew_rpt.sql > $TEMPDIR/clmrcvy_crfacets_cob_hew_rpt.sql
#$SCRIPTSDIR/run_sqlFile.sh -d $TEMPDIR -f 'clmrcvy_crfacets_cob_hew_rpt.sql' -l $logFileName


#STEP-10 Check for errors in reporting query

       err_cnt=`grep -i "ERROR" $logFileName | wc -l`
       if [ $err_cnt -ne 0 ]
       then
               echo "ERROR - while running 1 or more sqlFiles" >> $logFileName
               report_status  -a $attachFile -n $notifyInd -s 'FAILURE 102'
               exit 102
       fi
       echo 'Reporting queries for all requirements completed successfully'  >> $logFileName

#        if [ "$acrInd" == 'y' ]
#       then

#              echo 'Preparing to load Suspect Claim Table .. '  >> $logFileName

#                $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f 'clmrcvy_crfacets_hew_comp_suspct_clm.sql' -l $logFileName
#                rt_cd=$?
#               if [ $rt_cd -eq 0 ]
#              then
#                     echo 'Suspect Claim Table Loaded successfully'  >> $logFileName
#            else
#                   exit $rt_cd
#          fi

#        fi


# STEP-11  If all STEPS above complete succesfully, end the program with a return code of 0

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $scriptName Ended Normally in $GP_ENV at: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1

        report_status  -a $attachFile -n $notifyInd -s 'SUCCESS'


#------------------------------------------ END MAIN SCRIPT ---------------------------------------------#