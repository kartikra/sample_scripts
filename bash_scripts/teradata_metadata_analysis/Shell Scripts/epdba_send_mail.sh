#!/usr/bin/ksh
#---------------------------------- Start of Script epdba_send_mail.sh -----------------------------------------------#
#######################################################################################################################
#  Filename:  epdba_send_mail.sh
#  
#  Category:  Generic Utiltiy to Send Notification Email with attachement
#  PARAMETER(s): 
#               [ jobStatus ]   :-               
#               [ jobDesc ]     :-      
#               [ mailContent ] :-               
#               [ attachFile ]  :-               
#               [ techTeam ]    :-      
#               [ opsTeam ]    :-      
#               [ mgmtTeam ]    :-      
#               [ etlTeam ]    :-      
#               [ reportingTeam ]    :-      
#               [ miscTeam ]    :- 
#
#--------------------------------------------------------------------------------------------------
#          Examples for Invoking Shell  :-  
#
# epdba_send_mail.sh -s 'SUCCESS' -d 'GA Migration Run' -b 'emailMsg.dat' -a 'attachList.dat' -t 'cd_bio_dba' -o 'cd_bio_ops' -m 'cd_bio_leads'
#
#---------------------------------------GLOBAL VARIABLES----------------------------------------------------------#


#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#



# STEP-1 Read Input Parameters

        while getopts s:d:b:a:t:m:o:e:r:x:c: par
        do      case "$par" in
                s)      jobStatus="$OPTARG";;
                d)      jobDesc="$OPTARG";;
		b)      mailContent="$OPTARG";;
                a)      attachFile="$OPTARG";;
                t)      techTeam="$OPTARG";;
		m)      mgmtTeam="$OPTARG";;
                o)      opsTeam="$OPTARG";;
                e)      etlTeam="$OPTARG";;
                r)      reportingTeam="$OPTARG";;
                x)      miscTeam="$OPTARG";;
		c)	regionContact="$OPTARG";;

                [?])    echo "Correct Usage -->  epdba_send_mail.sh -s <jobStatus> -d <jobDesc> -b <Email Body> -a <List of attachements> -t <tech team email list>" 
                        exit 999;;
                esac
        done


# STEP-2 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi		
		
# STEP-3 Get the List of the recepients and create subject line
		DISTDIR=$HOMEDIR/scripts/parm
		to=""   
		
        if [ -f $DISTDIR/"$techTeam".dl ]
        then
                count=`cat $DISTDIR/$techTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst1=`cat $DISTDIR/$techTeam.dl | xargs -n $rowCount`
                to="$to $lst1"
        fi
		if [ -f $DISTDIR/$mgmtTeam.dl ]
        then
                count=`cat $DISTDIR/$mgmtTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst3=`cat $DISTDIR/$mgmtTeam.dl | xargs -n $rowCount`
                to="$to $lst3"
        fi
        if [ -f $DISTDIR/$opsTeam.dl ]
        then
                count=`cat $DISTDIR/$opsTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst2=`cat $DISTDIR/$opsTeam.dl | xargs -n $rowCount`
                to="$to $lst2"
        fi
       if [ -f $DISTDIR/"$etlTeam".dl ]
        then
                count=`cat $DISTDIR/$etlTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst4=`cat $DISTDIR/$etlTeam.dl | xargs -n $rowCount`
                to="$to $lst4"
        fi
		if [ -f $DISTDIR/$reportingTeam.dl ]
        then
                count=`cat $DISTDIR/$reportingTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst5=`cat $DISTDIR/$reportingTeam.dl | xargs -n $rowCount`
                to="$to $lst5"
        fi
        if [ -f $DISTDIR/$miscTeam.dl ]
        then
                count=`cat $DISTDIR/$miscTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst6=`cat $DISTDIR/$miscTeam.dl | xargs -n $rowCount`
                to="$to $lst6"
        fi
		if [ ! -z "$regionContact" ]
		then
		        count=`cat $DISTDIR/cd_bio_rsc.dl | grep -i "$regionContact" | cut -f2 -d '|' | wc -l`
                rowCount=`expr $count + 1`
                lst7=`cat $DISTDIR/cd_bio_rsc.dl | grep -i "$regionContact" | cut -f2 -d '|' xargs -n $rowCount`
                to="$to $lst7"
		fi

    subject="$jobStatus : $jobDesc"


# STEP-4 Send the Attachement To the recepients

        if [  -z "$to" ]
        then
                #echo "No Valid Email Address"
                #exit 100
                to="kartik.ramasubramanian@didi.com"
        fi

		attachVar="(cat $TEMPDIR/email_msg.dat"
		attachCnt="0"
		if [ -f "$attachFile" ]
		then
			cat $attachFile | while read -r line; do
			
				folderName=`echo $line | cut -f1 -d'|'`
				fileName=`echo $line | cut -f2 -d'|'`
				attachmentName=`echo $line | cut -f3 -d'|'`
				if [ -z "$attachmentName" ]
				then
					attachmentName=$fileName
				fi
			
				if [ -f "$folderName/$fileName" ]
				then
					# Replace $ with Ctrl-M
					sed 's/$//g' $folderName/$fileName > $TEMPDIR/temp_"$fileName"
					attachVar=""$attachVar";uuencode $TEMPDIR/temp_"$fileName" $attachmentName"
					attachCnt=`expr $attachCnt + 1`
				fi
			done
		fi
		attachVar="$attachVar)"
		#echo $attachVar 
			
		rm -f $TEMPDIR/email_msg.dat
		
		if [ -f "$mailContent" ]
		then
			cat $mailContent >> $TEMPDIR/email_msg.dat
			echo "" >> $TEMPDIR/email_msg.dat
		fi
		echo "Thanks," >> $TEMPDIR/email_msg.dat
		echo "Clarity DBA Support" >> $TEMPDIR/email_msg.dat
		echo "" >> $TEMPDIR/email_msg.dat
		echo "This ia an automated email. Please contact \"CD BIO IM APP DBA-IREG@didi.com\" for any issues." >> $TEMPDIR/email_msg.dat
		echo "" >> $TEMPDIR/email_msg.dat
		echo "" >> $TEMPDIR/email_msg.dat
		if [ $attachCnt -ne 0 ]
		then
			echo "" >> $TEMPDIR/email_msg.dat
			echo " Attachements - " >> $TEMPDIR/email_msg.dat
		fi
		echo "" >> $TEMPDIR/email_msg.dat
		echo "" >> $TEMPDIR/email_msg.dat

		echo "#!/usr/bin/ksh" > $TEMPDIR/email.sh
		echo "$attachVar | mailx -s \"$subject\" \"$to\"" >> $TEMPDIR/email.sh
		
		
		# Run the script to send email
		chmod 775 $TEMPDIR/email.sh
		$TEMPDIR/email.sh
		
		# Cleanup All Temp Files
		if [ -s "$attachFile" ]
		then
			count=`cat $attachFile | wc -l`
			if [ $count -gt 0 ]
			then
				cat $attachFile | while read -r line; do
					fileName=`echo $line | cut -f2 -d'|'`
					rm -f $TEMPDIR/temp_"$fileName"
				done
			fi
		fi
        
#---------------------------------- End of Script epdba_send_mail.sh -----------------------------------------------#
