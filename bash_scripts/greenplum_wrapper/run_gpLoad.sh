#!/usr/bin/ksh

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#

#     USAGE : ksh  run_gpLoad.sh -s /gpfs01/dev/edl/pcenterdata/TgtFiles/ff_clmrc_h1_1_edw_clm.dat -t clm_rcvy_lz.edward_clm_lz -e clm_rcvy_lz.edward_gpload_err
#---------------------------------------------------------------------------------------------------------------------------------------------#

#CODE="/gpfs01/dev/edl/code"
#PMDIR="/gpfs01/dev/edl/pcenterdata"


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

        while getopts s:t:e:f:r:a:y:j:l: par
        do      case "$par" in
                s)      srcFileName="$OPTARG";;
                t)      gp_trgt_table_name="$OPTARG";;
                e)      gpErrTable="$OPTARG";;
                f)      gpErrLimit="$OPTARG";;
                r)      fullRefreshInd="$OPTARG";;
                a)      analyzeInd="$OPTARG";;
                y)      ymlFile="$OPTARG";;
		j)      jobCode="$OPTARG";;
                l)      logFileName="$OPTARG";;
                [?])    echo "Correct Usage -->  ksh run_gpLoad.sh -s <srcFileName> -t <gpTrgtTable> -e <gpErrTable> -f <gpErrLimit> -r <fullRefreshInd> -a <analyzeInd> -y <ymlFile> -j <jobCode> -l <logFileName>" 
                        exit 999;;
                esac
        done


#STEP-3 Check for Mandatory Parameters

        shellName=`basename $0`


        trgtName=`echo "gp_trgt_table_name" | cut -f2 -d'.'`
        if [   ! -z "$ymlFile" ]
        then
                ymlFileName=$CTLDIR/$ymlFile
        else
                ymlFileName=$CTLDIR/$trgtName.yml
        fi



        if [  ! -f "$logFileName" ]
        then
                today=`date +"%Y_%m_%d_%H_%M_%S"`
                logFileName="$LOGDIR/$shellName_$ymlFile-$today"
                touch $logFileName
        fi
        chmod 775 $logFileName
        

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $shellName Started on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1

        if [  ! -f "$srcFileName" ]
        then
                echo "ERROR - Invalid Parameter - srcFileName" >> $logFileName
                exit 912
	else
		wc -l $srcFileName > $TEMPDIR/$shellName_$ymlFile &
        fi
        if [   -z "$gp_trgt_table_name" ]
        then
                echo "ERROR - Missing Required Parameter - gp_trgt_table_name" >> $logFileName
                exit 911
        fi
        if [   -z "$gpErrTable" ]
        then
                echo "ERROR - Missing Required Parameter - gpErrTable" >> $logFileName
                exit 911
        fi


	schema_name=`echo $gp_trgt_table_name | cut -f1 -d'.'`
	table_name=`echo $gp_trgt_table_name | cut -f2 -d'.'`
	
	if [ $schema_name == "clm_rcvy_lz" ]
	then
		schema_name=$GPLZSCHEMA
	fi
	if [ $schema_name == "clm_rcvy_stg" ]
	then
		schema_name=$GPSTGSCHEMA
	fi
	if [ $schema_name == "clm_rcvy" ]
	then
		schema_name=$GPRPTSCHEMA
	fi
	if [ $schema_name == "clm_rcvy_buss" ]
	then
		schema_name=$GPBUSSCHEMA
	fi
	
	gpTrgtTable=$schema_name.$table_name
	echo "Target Table is $gpTrgtTable" >> $logFileName


        if [   -z "$gpErrLimit" ]
        then
                gpErrLimit="10000"
        else
                gpErrLimit=`expr $gpErrLimit`
                rt_cd=$?
                if [ $rt_cd -ne 0 ]
                then
                      gpErrLimit="10000"
                fi
        fi

        

#STEP-4 Create the YML File Dynamically

        echo "VERSION: 1.0.0.1" >  $ymlFileName
        echo "DATABASE: $GPDB" >>  $ymlFileName
        echo "USER: gpadmin" >>  $ymlFileName
        echo "HOST: $GPHOST" >>  $ymlFileName
        echo "PORT: 5432" >>  $ymlFileName
        echo "GPLOAD:" >>  $ymlFileName
        echo "   INPUT:" >>  $ymlFileName
        echo "     - SOURCE:" >>  $ymlFileName
        echo "         FILE:" >>  $ymlFileName
        echo "           - $srcFileName" >>  $ymlFileName
        echo "     - FORMAT: text" >>  $ymlFileName
        echo "     - HEADER: false" >>  $ymlFileName
        echo "     - DELIMITER: '|' " >>  $ymlFileName
        echo "     - NULL_AS: '' " >>  $ymlFileName
        echo "     - ESCAPE: "OFF"" >>  $ymlFileName
        echo "     - ERROR_LIMIT: $gpErrLimit" >>  $ymlFileName
        echo "     - ERROR_TABLE: $gpErrTable" >>  $ymlFileName
        echo "   OUTPUT:" >>  $ymlFileName
        echo "     - TABLE: $gpTrgtTable" >>  $ymlFileName
        echo "     - MODE: INSERT" >>  $ymlFileName

        if [  "$fullRefreshInd" == "Y" ] || [  "$analyzeInd" == "A" ]
        then
                echo "   SQL:" >>  $ymlFileName
        fi
        if [  "$fullRefreshInd" == "Y" ]
        then
                echo "      - BEFORE: \"TRUNCATE $gpTrgtTable;\" " >>  $ymlFileName
        fi
        if [  "$analyzeInd" == "A" ]
        then
               echo "      - AFTER: \"ANALYZE $gpTrgtTable;\" " >>  $ymlFileName
        fi
        
        echo "   " >>  $ymlFileName
        chmod 775 $ymlFileName


# STEP-5 Perfrom GPLOAD

        gpload -f $ymlFileName -h $GPHOST -U $GPUSER -d $GPDB >> $logFileName  2>&1


# STEP-6 Check Status of GPLOAD

        err_cnt=`grep "ERROR" $logFileName | wc -l`
        if [ $err_cnt -ne 0 ]
        then
               echo "ERROR - while performing GPLOAD For $gpTrgtTable" >> $logFileName
               exit 908
        else
               echo "GPLOAD For $gpTrgtTable completed normally with code - $rt_cd" >>  $logFileName
        fi



# STEP-7 Get Input File Count (from the background)
	rows_read=0
        rows_read=`cat $TEMPDIR/$shellName_$ymlFile | head -1 | cut -f1 -d"/" | sed 's/ //g'`
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Run unix command"
                err_cd=801
                exit 801
        fi
        rm -f $TEMPDIR/$shellName_$ymlFile
	echo " Rows Read for $jobCode : $rows_read " >> $logFileName

# STEP-8 Get Success Count and Error Count

	#err_count=`cat $logFileName | grep -i "|WARN|" | cut -f3 -d'|' | cut -f1 -d' '`
	#trgt_count=`cat $logFileName | grep -i "|INFO|rows Inserted" | cut -f2 -d'=' | sed 's/ //g'`
	#echo " Rows Error   for $jobCode : $err_count " >> $logFileName
	#echo " Rows Written for $jobCode : $trgt_count " >> $logFileName


        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $shellName Ended on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1

#-------------------------------------- END MAIN SCRIPT ---------------------------------------------#
