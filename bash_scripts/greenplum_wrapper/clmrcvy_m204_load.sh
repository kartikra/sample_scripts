#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_m204_load.sh ------------------------------------#

#STEP-1 Read Parameters


#STEP-2 Run Profile File

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi
        
#STEP-3 Create Log File for current run 

        bkup=`date +"%Y_%m"`
        today=`date +"%Y_%m_%d_%H_%M_%S"`
        shellName=`basename $0`
        logFileName=$LOGDIR/$shellName-$today.log
        touch $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Create LogFile"
                exit 901
        fi

        chmod 775 $logFileName

        echo "**************************************************************************************" >> $logFileName 2>&1
        echo "Program $shellName Started on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
        echo "**************************************************************************************" >> $logFileName 2>&1


# STEP-4  Get file from mainframe

        $SCRIPTSDIR/clmrcvy_get_mfrm_file.sh \'GNAP.NONX.GNA420MM.GNA42016.OVPACTY\(0\)\' $logFileName $WGSHOST clmrcvy_wgsmf_logon.ctrl

        cd $SRCFILES
        mv \'GNAP.NONX.GNA420MM.GNA42016.OVPACTY\(0\)\' clmrcvy_wgsmf_m204_monthly.dat


# STEP-5  Delete data for same month if loaded previously

        cleanup="DELETE FROM clm_rcvy_lz.m204_monthly_feed_lz WHERE var_rpt_month =  CAST( (to_char((current_date - interval '1 month'),'YYYY-MM') || '-01') AS DATE);"
        echo $cleanup > $SQLDIR/m204_cleanup.sql
        
        chmod 775 m204_cleanup.sql

        $SCRIPTSDIR/run_sqlFile.sh -f m204_cleanup.sql -d  $SQLDIR -l $logFileName


# STEP-6  Fixed-Width to Delimited

        cd $SRCFILES
        cp clmrcvy_wgsmf_m204_monthly.dat "m204_monthly_$bkup.dat"

        awk '/$/{
                
                print substr($0, 1, 10) "|" substr($0, 11, 10) "|" substr($0, 21,9) "|" substr($0, 30, 2) "|"  \
                substr($0, 32, 10) "|"  substr($0, 42, 9) "|" substr($0, 51, 3) "|" substr($0, 54, 2) "|"  \
		substr($0, 56, 15) "|" substr($0, 71, 8) "|"  substr($0, 79, 8) "|" substr($0, 87, 8) "|"   \
		substr($0, 95, 8) "|" substr($0, 103, 1) "|"  substr($0, 104, 1) "|" substr($0, 105, 8) "|"   \
	        substr($0, 113, 4) "|" substr($0, 117, 1) "|"  substr($0, 118, 1) "|" substr($0, 119, 1) "|"   \
		substr($0, 120, 1) "|" substr($0, 121, 22) "|"  substr($0, 143, 22) "|" substr($0, 165, 22) "|"   \
                substr($0, 187, 22) "|" substr($0, 209, 22) "|"  substr($0, 231, 22) "|" substr($0, 253, 22) "|"   \
		substr($0, 275, 22) "|" substr($0, 297, 22) "|"  substr($0, 319, 1) "|" substr($0, 320, 3) "|"   \
                substr($0, 323, 1) "|" substr($0, 324, 2) "|"  substr($0, 326, 9)  "|"  substr($0, 335)
        }'  clmrcvy_wgsmf_m204_monthly.dat > tmp_clmrcvy_wgsmf_m204_monthly.csv 2>> $logFileName


# STEP-7  Replace Trailing and Leading Spaces

        sed -e 's/ *| */|/g'  tmp_clmrcvy_wgsmf_m204_monthly.csv > clmrcvy_wgsmf_m204_monthly.csv 2>> $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR while trimming spaces in file tmp_clmrcvy_wgsmf_m204_monthly" >> $logFileName
                exit 102
        fi


#---------------------- End of Script clmrcvy_m204_load.sh ------------------------------------#
