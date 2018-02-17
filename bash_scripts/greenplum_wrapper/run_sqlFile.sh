#!/usr/bin/ksh
#---------------------------------- Start of Script run_sqlFile.sh -----------------------------------------------#
#######################################################################################################################
#  Filename:  run_sqlFile.sh
#  
#               
#  PARAMETER(s): 
#               [ sqlFileName ] :- Name of the sql File         MANDATORY 
#               [ sqlFileDir ]  :- Directory of Sql File        MANDATORY 
#               [ logFileName ] :- Name of Log File             MANDATORY 
#
#
#  Othe Shell(s) Invoked from this shell: None
#
#  Description:  
#         This script is a generic shell script which takes in sqlFile Directory, SQL File Name,
#         and Log File Name as input. The script will call psql utility and pass in the sql file name
#         as the paramter. The results and/or errors from psql are logged in the logfile
#
#  Main Processing:
#
#     Step 1.   Run the .profile file for clm_rcvy. Abort script if profile file does not run successfully
#
#     Step 2.   Get all the input Parameters using getopts. 
#
#     Step 3.   Run the queries in the sql file. Abort the script if the logfile is not created.
#    
#  Final Output: The sql queries listed in the log file are run one after the other
#
#
#  Return Code:   If Return Code is not equal to 0 
#                 check the log file for execptions encountered during processing
#
#  Log File Name:  Results and errors are logged in the log filename passed in as a parameter
#                  
#
#--------------------------------------------------------------------------------------------------
#          Examples for Invoking Shell  :-  
#                   ksh run_sqlFile.sh -d /tmp/usrs -f abc.dat -l log1.log
#
#######################################################################################################################
#---------------------------------------GLOBAL VARIABLES----------------------------------------------------------#

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#

# STEP-1 Define Global Variables by running the profile file

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting"
                exit 902
        fi


        

# STEP-2 Read Input Parameters

        while getopts f:d:l: par
        do      case "$par" in
                f)      sqlFileName="$OPTARG";;
                d)      sqlFileDir="$OPTARG";;
                l)      logFileName="$OPTARG";;
                [?])    echo "Correct Usage -->  run_sqlFile.sh -d <sqlFileDir> -f <sqlFileName> -l <logFileName>" 
                        exit 999;;
                esac
        done


        
        if [ "$GP_ENV" != "DEV" ] && [ "$GP_ENV" != "TEST" ] && [ "$GP_ENV" != "PROD" ]
        then
                echo "ERROR - Invalid GP_ENV Value passed to run_sqlFile.sh - $GP_ENV" >> $logFileName
                exit 911
        fi

	if [ ! -f "$sqlFileDir/$sqlFileName" ]
	then
		echo "ERROR - File $sqlFileName Not Found" >> $logFileName
		exit 904
	fi


# STEP-3 Run the queries in the sql file. Record all results in the logfile
#        Abort Script on any errors


        rm -f $TEMPDIR/env-$sqlFileName
        echo  "SET SEARCH_PATH=$GPLZSCHEMA,$GPSTGSCHEMA,$GPRPTSCHEMA,$GPBUSSCHEMA;" > $TEMPDIR/env-$sqlFileName
        cat $sqlFileDir/$sqlFileName >> $TEMPDIR/env-$sqlFileName
        
        psql -h $GPHOST -U $GPUSER -d $GPDB -f $TEMPDIR/env-$sqlFileName -t >> $logFileName  2>&1
        if [ $rt_cd -ne 0 ]
        then
                echo "SQL in $sqlFileName completed abruptly with code - $rt_cd" >>  $logFileName
                rm -f $TEMPDIR/env-$sqlFileName
                exit 908
        else
                err_cnt=`grep "ERROR" $logFileName | wc -l`
                fat_cnt=`grep "FATAL" $logFileName | wc -l`

                if [ $err_cnt -ne 0 ] || [ $fat_cnt -ne 0 ]
                then
                       echo "ERROR - while running sqlFile env-$sqlFileName" >> $logFileName
                       rm -f $TEMPDIR/env-$sqlFileName
                       exit 908
                else
                       echo "SQL in $sqlFileName completed normally with code - $rt_cd" >>  $logFileName
                fi
                rm -f $TEMPDIR/env-$sqlFileName
        fi

        

#-------------------------------------- END MAIN SCRIPT ---------------------------------------------#

#---------------------------------- End of Script run_sqlFile.sh -----------------------------------------------#

