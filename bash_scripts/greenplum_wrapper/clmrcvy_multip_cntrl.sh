#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_multip_cntrl.sh ------------------------------------#

#CODE="/gpfs01/dev/edl/code"
#PMDIR="/gpfs01/dev/edl/pcenterdata"



#STEP-1 Run Profile File
        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi

     
#STEP-2 Create Log File for current run

        today=`date +"%Y_%m_%d_%H_%M_%S"`
	scriptName=`basename $0`

        logFileName="$LOGDIR/$scriptName-$today.log"
        attachFile="$scriptName-$today.log"

        touch $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Create LogFile"
                exit 903
        fi
	
	chmod 775 $logFileName

        echo "**************************************************************************************" > $logFileName 2>&1
        echo "Program $shellName Started on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1



# STEP-3 Read Input File


	if [ -f "$PMDIR/TgtFiles/ff_multip_ctl_data.out" ]
	then
              totl_cnt=`cat $PMDIR/TgtFiles/ff_multip_ctl_data.out | cut -d'|' -f1`
	      curr_mnth=`cat $PMDIR/TgtFiles/ff_multip_ctl_data.out | cut -d'|' -f3`
	      curr_day=`cat $PMDIR/TgtFiles/ff_multip_ctl_data.out | cut -d'|' -f4`
	else
	        echo "Input file cannot be found, Exiting" >> $logFileName 2>&1
		exit 903
	fi


# STEP-4   Touch the trigger file if  -
#             1) the row count is not equal to 0 and 
#             2) the trigger file was not been touched this month

	
	if [ -f "$PMDIR/TrigFiles/clmrcvy_multip_clm.trig" ]
	then
              last_touch_mnth=`ls -ltr $PMDIR/TrigFiles/clmrcvy_multip_clm.trig | tr [a-z] [A-Z] | grep "$curr_mnth" | wc -l | sed -e 's/ //g'`
	else
	      last_touch_mnth="0"
	fi
	
	
	echo "Total Count    : $totl_cnt" >> $logFileName
        echo "Current Month  : $curr_mnth" >> $logFileName
	echo "Last Touch     : $last_touch_mnth" >> $logFileName


	if [ $totl_cnt != "0" ] && [ "$last_touch_mnth" == "0" ]
        then  
                touch $PMDIR/TrigFiles/clmrcvy_multip_clm.trig
		echo "Trigger File touched"  >> $logFileName
        fi



# STEP-5   Issue an alert if it's 20th of the month and data is still not refreshed on MULTIP

       if [ "$totl_cnt" == "0" ] && [ "$curr_day" == "20" ]
       then
		`sed 's/$//g' $logFileName > $TEMPDIR/mail.dat`
	         uuencode $TEMPDIR/mail.dat $scriptName-$today.log |  \
	          mailx -s "DATA REFRESH ALERT : 20th of $curr_mnth - Multip Data Not Refreshed " \
	         "dl-BABW-Claims-Accuracy-Tech@wellpoint.com " 
       fi


        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $shellName Ended on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1


#---------------------- End of Script clmrcvy_multip_cntrl.sh ------------------------------------#

