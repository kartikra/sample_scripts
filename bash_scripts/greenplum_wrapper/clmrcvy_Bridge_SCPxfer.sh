##################################################################
## This script is to be used in conjuction with a parameter file##
##              SCP_FILE_LIST                                   ##
## Its purpose is to:                                           ##
##    a>. provide some basic error trapping                     ##
##    b>. transfer the data to the bridge server using a        ##
##        secure copy (scp)                                     ##
##                                                              ##
## Support files used:                                          ##
##   <type>.env.parm            -environment variables          ##
##   <type>_SCP_FILE_LIST       -file list of files to transfer ##
##                                                              ##
## Command line argument                                        ##
##   $1 Required. This argument is used to identify the file    ##
##      list name and the .env file.                            ##
##   $2 This is an optional extract date. If no date is passed  ##
##      then CCYYMM - 1 is assumed                              ##
##      example:  mysample.!date!.out <-- file with mask        ##
##      after:    mysample.200806.out <-- where $2 = 200806     ##     
##                                                              ##
##################################################################
##
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
                s)      dateList="$OPTARG";;
                n)      notifyInd="$OPTARG";;
                a)      acrInd="$OPTARG";;
                [?])    echo "Correct Usage -->  clmrcvy_cs90_asst_surg.sh -r <rqtList> -s <dateList> -a <acrInd> -n <notify>"
                        exit 999;;
                esac
        done

##STEP-3 Create Log File for current run

        today=`date +"%Y_%m_%d_%H_%M_%S"`
        scriptName=`basename $0`

        logFileName="$LOGDIR/$scriptName-$today.log"
        attachFile="$scriptName-$today.log"
        SCP_LOGFILE="$LOGDIR/${rqtList}_SCPxfer_details.log.$logdate"

        echo "START" >> $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Create LogFile"
                exit 901
        fi

#STEP-4 Validate Input Parameters

        if [ $GP_ENV != "DEV" ] && [ $GP_ENV != "TEST" ] && [ $GP_ENV != "PROD" ]
        then

                echo "ERROR - Invalid GP_ENV Value - $GP_ENV" >> $logFileName
                exit 911
        fi


        if [ -z "$dateList" ]
        then
                echo "ERROR - Missing Required Parameter - dateList " >> $logFileName
                echo "Correct Usage --> clmrcvy_Bridge_SCPxfer.sh -r <rqtList> -d <recovRefDate> -s <dateList>"  >> $logFileName
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



##Step 5 Set FTP Parameters

        FTP_PROF=$CODE/clmrcvy_gp/scripts/SCPxfer.profile
        . $FTP_PROF $rqtList > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "SCP Profile file cannot be found, Exiting"
                exit 902
        fi

## Step Set Date
#
###fill datemask..assume it is for the previous month
###This date will be used if arg2 not passed in at run time
   this_month=$(date +%Y%m)                                                
   if [[ $this_month = *01 ]] ; then                                         
      datemask=$(( $this_month - 89 ))   # e.g 199812 = 199901 - 89          
   else                                                                    
      datemask=$(( $this_month - 1 ))                                        
   fi                                                                      
##
##
if [[ -n $arg2 ]] ; then             
  filestr=$dateList
else                            
  filestr=$datemask
fi


## Print run stats to the log file
echo "*************************************" >> $logFileName
echo "jobname=:           $scriptName" >> $logFileName
echo "date=:              $today" >> $logFileName
echo "logfile directory=: $LOGDIR" >> $logFileName
echo "logfile name=:      $logFileName" >> $logFileName
echo "SCP logfile name=:  $SCP_LOGFILE" >> $logFileName
echo "LOG_OUTPUT_DIR=:    $LOG_OUTPUT_DIR" >> $logFileName
echo "SOURCE_FILE_DIR=:   $SOURCE_FILE_DIR" >> $logFileName
echo "ETL_DIR=:           $ETL_DIR" >> $logFileName
echo "BRIDGE_SERVER=:     $BRIDGE_SERVER" >> $logFileName
echo "BRIDGE_PATH=:       $BRIDGE_PATH" >> $logFileName
echo "BRIDGE_FOLDER=:     $BRIDGE_FOLDER" >> $logFileName
echo "Job Parm Type=:     $Type" >> $logFileName
echo "Job argument #2=:   $filestr" >> $logFileName
echo "*************************************" >> $logFileName

cd ${SOURCE_FILE_DIR}

echo "$jobname start loop with file list ${Type}_SCP_FILE_LIST" >> $logFileName
##################################################################
## The format of each line in this list is:                     ##
## data|compress type                                           ##
##                                                              ##
## data    = the name of the data file                          ##
## !date!  = the extract date supplied at run time or the       ##
##           default built if no date is passed                 ##
##           (assumed to be current yr and month - 1)           ##
##           Format is YYYYMM                                   ##
## compress type= The compression type of the file. (gz, z)     ##
##################################################################
##
scplist=${SCP_FILE_NAME}_$filestr
mv  ${SCP_FILE_NAME}.txt  ${SCP_FILE_NAME}_$filestr
   
   echo "******File selected for transfer******" >> $logFileName
   echo "FileName=:            ${FileName}" >> $logFileName
##
   FileName=$(echo $FileName | sed "s/!date!/$filestr/")
   echo "FileName after mask=: ${FileName}${sufx}" >> $logFileName
### Make sure file exists and is not empty
   file=$SOURCE_FILE_DIR/${FileName}${sufx}
   if test ! -s "$file" ; then
      echo "ERROR: File list $file does not exist or is empty" >> $logFileName
   else
      echo "File list file to be copied using-> scp -qB ${file} ${BRIDGE_SERVER}:${BRIDGE_PATH}/${BRIDGE_FOLDER}" >> $logFileName
##
## Execute the SCP of the files selected
## Run in (B)atch mode and disable progress meter (q)
## Use option 'v' to debug.
## All output goes to $SCP_LOGFILE
     scp -qB ${file} ${BRIDGE_SERVER}:${BRIDGE_PATH}/${BRIDGE_FOLDER} > scp.stdout 2> ${SCP_LOGFILE}

## Test the return code to see if transfer worked
     steprc=$?
     if [ $steprc -ne 0 ]; then
        echo "ERROR: SCP failed on file $file" >> $logFileName
     else
        echo "SUCCESS: file $file transferred" >> $logFileName
     fi
   fi

####################### 
##Out of loop. Grep the $logFileName for ERROR to indicate job status
echo "*************************************" >> $logFileName
chkquery=""                                                                    
chkquery="$(/bin/cat $logFileName | grep "ERROR")"                                 
if [[ $chkquery != "" ]] ; then
    echo "jobname $jobname has ERRORs-exit 1" >> $logFileName
    exit 1
fi
echo "jobname $jobname completed-exit 0" >> $logFileName
exit 0
############################################ END OF SCRIPT ##################################################
