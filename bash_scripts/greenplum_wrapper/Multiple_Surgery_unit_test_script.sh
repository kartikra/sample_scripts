#!/usr/bin/ksh
#---------------------------------- Start of Script Multiple_Surgery_unit_test_script.sh -----------------------------------------------#
#######################################################################################################################
#  Filename:  Multiple_Surgery_unit_test.sh
#  Category:  Landing Zone to Staging (lztostg)
#  Description:  
#  Parameter(s): 
#
#  Other Scripts Invoked from this shell:
#  1. sqlFile.sh    :- Processes sqlFile and records results in a logile.
#                                  Calling shell must pass sqlFile Directory, sqlFile Name
#                                  and logFile Name
#
#  Function : 
#  Final Output:  Records will be inserted into the following tables -  
#             
###########################################################################################################
#  VERSION	CREATED/MODIFIED BY	    DATE		COMMENTS
#  1.0          Ram Chandra Saurabh        09/24/2010         Initial Draft
#######################################################################################################################
#---------------------------------------GLOBAL VARIABLES----------------------------------------------------------#


#-------------------------------------- BEGIN  FUNCTIONS ---------------------------------------------------------#

email_success_stts()
{

 notifyInd=$1
 if [ $notifyInd == 'y' ]
     then
	uuencode $log_file| mailx -s 'WGS/Star  Multiple Surgery Unit-test Script Success' RamChandra.Saurabh@mycompany.com krishnadas.sukumaran@mycompany.com riti.jose@mycompany.com < $log_file	
	if [ $? != 0 ]
	 then
	  echo "Job Sending Success Email notification Failed .... Aborting Script" >> $log_file
	  exit 902
        else
          echo "Success Email notification sent Sucessfully .... Proceeding with Script" >> $log_file
	fi
 fi

}

email_result()
{

 notifyInd=$1
 if [ $notifyInd == 'y' ]

     then
 cat /data/clm_rcvy/data/Mu_Surg_result.txt | mailx -s 'WGS/Star Multiple Surgery Unit test result' ac15417@mycompany.com \  ab89149@mycompany.com ac13928@mycompany.com ab88757@mycompany.com ac22717@mycompany.com ac15418@mycompany.com \
ab97387@mycompany.com ab88757@mycompany.com
  	
       if [ $? != 0 ]
	 then
	  echo "Job Sending Success Email notification Failed .... Aborting Script" >> $log_file
	  exit 902
        else
          echo "Success Email notification sent Sucessfully .... Proceeding with Script" >> $log_file
	fi
 fi

}


email_error_stts()
{

 notifyInd=$1
 if [ $notifyInd == 'y' ]
     then
	uuencode $log_file  | mailx -s 'WGS/Star Multiple Surgery Unit-test Script Error' RamChandra.Saurabh@mycompany.com < $log_file	
	if [ $? != 0 ]
	 then
	  echo "Job Sending Error Email notification Failed .... Aborting Script" >> $log_file
	  exit 1
        else
          echo "Error Email notification sent Sucessfully .... Aborting Script" >> $log_file
	fi
 fi

}


Display_result()

{

touch /data/clm_rcvy/data/Mu_Surg_result.txt

Echo "#######################################################" >> /data/clm_rcvy/data/Mu_Surg_result.txt

echo " WGS/STAR Multiple Surgery - Unit test result " >> /data/clm_rcvy/data/Mu_Surg_result.txt

echo "#######################################################" >> /data/clm_rcvy/data/Mu_Surg_result.txt

psql -A  -o /data/clm_rcvy/data/Mu_Surg_result.txt -c "select * from clm_rcvy_stg.edward_Multiple_Surg_unit_test_result"


}


#--------------------------------------- END  FUNCTIONS ---------------------------------------------------------#

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#


# --------------------------------------------------------------------------#
# STEP-1 :- Run the .profile file for clm_rcvy to set all environment variables #
#	    Create the Script Log-File
# --------------------------------------------------------------------------#

USR_PROF=/export/home/clm_rcvy/clmrcvy.profile
. $USR_PROF > /dev/null 2>&1
rt_cd=$?
if [ $rt_cd -ne 0 ]
then  
	echo "Profile file cannot be found, Exiting"
	exit 902
fi


notifyInd=y
exec_str=`date +%Y%m%d-%H:%M:%S`
script_nm=`basename $0`
log_file=${LOGDIR}/${script_nm}_$exec_str.log

chmod 775 log_file

echo "-----------------------------------------------------------" >> $log_file 2>&1
echo "Script ${script_nm}.sh Started on: $exec_str " >> $log_file 2>&1
echo "-----------------------------------------------------------\n" >> $log_file 2>&1

#------------------------------------------------------------------------------------------#
# STEP-2 Read Input Parameters
#------------------------------------------------------------------------------------------#
       # while getopts e: par
        #do      case "$par" in
               
         #       e)      ENV="$OPTARG";;
          #      [?])    echo "Correct Usage -->  Multiple_Surgery_unit_test_script.sh -e <ENV> " 
           #             exit 999;;
           #    esac
       # done 

 

#STEP-3 Validate Input Parameters

        if [ $GP_ENV != "DEV" ] && [ $GP_ENV != "TEST" ] && [ $GP_ENV != "PROD" ]
        then
                echo "ERROR - Invalid ENV Value - $GP_ENV" >> $log_File
                exit 911
        fi





# -----------------------------------------------------------------------------------------------#
# STEP-4 :- Call and execute the Queries for Unit testing Core Business Logics		   #
# -----------------------------------------------------------------------------------------------#

 echo "\n-----------------------------------------------------------------" >> $log_file
 echo "Starting Run of Negative Unit Test Cases for Multiple Surgery" >> $log_file
 echo "-----------------------------------------------------------------\n" >> $log_file
 

 psql -c "truncate table clm_rcvy_stg.edward_Multiple_Surg_unit_test_result"

 
 $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f WGS_Star_MS_Unit_test.sql -l $log_file
 

  err_cnt=`grep -i "ERROR" $log_file | wc -l`


if [ $err_cnt -gt 0 ]
  then
	echo "\nSQL: WGS_Star_MS_Unit_test.sql Failed" >>  $log_file
	email_error_stts  $notifyInd
	exit 1

  else 
	echo "\nSQL: WGS_Star_MS_Unit_test.sql Succeeded" >>  $log_file
       email_success_stts $notifyInd

  fi






Display_result


email_success_stts $notifyInd

email_result $notifyInd

 
# 
# #------------------------------------------ END MAIN SCRIPT ---------------------------------------------#


