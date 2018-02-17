#!/usr/bin/ksh
#---------------------------------- Start of Script send_mail.sh -----------------------------------------------#
#######################################################################################################################
#  Filename:  send_mail.sh
#  
#  Category:  Generic Utiltiy to Send Notification Email with attachement
#  PARAMETER(s): 
#               [ jobStatus ]   :-               
#               [ jobDesc ]     :-      
#               [ attachFile ]  :-               
#               [ logFileName ] :- 
#               [ techTeam ]    :-      
#               [ bussTeam ]    :-      
#               [ mgmtTeam ]    :-      
#
#--------------------------------------------------------------------------------------------------
#          Examples for Invoking Shell  :-  
#
# send_mail.sh -s 'SUCCESS' -d 'WGS-STAR' -a 'dup_crdw.log' -t 'wgs_dev' -b 'cob_bus' -m 'leads'
#
#---------------------------------------GLOBAL VARIABLES----------------------------------------------------------#


#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#

#CODE="/gpfs01/dev/edl/code"
#PMDIR="/gpfs01/dev/edl/pcenterdata"


# STEP-1 Read Input Parameters

        while getopts s:a:t:b:m:d: par
        do      case "$par" in
                s)      jobStatus="$OPTARG";;
                a)      attachFile="$OPTARG";;
                t)      techTeam="$OPTARG";;
                b)      bussTeam="$OPTARG";;
                m)      mgmtTeam="$OPTARG";;
                d)      jobDesc="$OPTARG";;

                [?])    echo "Correct Usage -->  send_mail.sh -s <jobStatus> -d <jobDesc> -a<attachFile> -l <logFileName>" 
                        exit 999;;
                esac
        done

        

# STEP-2 Define Global Variables by running the profile file

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting"
                exit 902
        fi

to=""



# STEP-3 Get the List of the recepients and create subject line
        
        if [ -f $SCRIPTSDIR/$techTeam.dl ]
        then
                count=`cat $SCRIPTSDIR/$techTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst1=`cat $SCRIPTSDIR/$techTeam.dl | xargs -n $rowCount`
                to=$to' '$lst1
        
        fi
        if [ -f $SCRIPTSDIR/$bussTeam.dl ]
        then
                count=`cat $SCRIPTSDIR/$bussTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst2=`cat $SCRIPTSDIR/$bussTeam.dl | xargs -n $rowCount`
                to=$to' '$lst2
        fi
        if [ -f $SCRIPTSDIR/$mgmtTeam.dl ]
        then
                count=`cat $SCRIPTSDIR/$mgmtTeam.dl | wc -l`
                rowCount=`expr $count + 1`
                lst3=`cat $SCRIPTSDIR/$mgmtTeam.dl | xargs -n $rowCount`
                to=$to' '$lst3
        
        fi

        subject="$jobStatus : $jobDesc RUN"
	echo $to >> $LOGDIR/$attachFile
	echo $subject >> $LOGDIR/$attachFile
        

# STEP-4 Send the Attachement To the recepients

        if [  -z "$to" ]
        then
                #echo "No Valid Email Address"
                #exit 100
                to="dl-BABW-Claims-Accuracy-Tech@mycompany.com"
        else

                `sed 's/$//g' $LOGDIR/$attachFile > $TEMPDIR/initv_mail.dat`
                uuencode $TEMPDIR/initv_mail.dat $attachFile |  \
                mailx -s "$subject" "$to" 

                #cat $LOGDIR/$attachement | sed 's/$//g' > temp.out | uuencode temp.out  $attachement |  \
                #mailx -s "$subject" "$add1" 
        fi
#---------------------------------- End of Script send_mail.sh -----------------------------------------------#
