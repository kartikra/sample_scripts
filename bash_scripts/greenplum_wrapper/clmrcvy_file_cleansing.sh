#!/bin/ksh

#CODE=/gpfs01/dev/edl/code/

# STEP-1 Run the .profile file for clm_rcvy to set all environment variables

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile clmrcvy.profile cannot be found, Exiting"
                err_cd=903
                exit 903
        fi

        File_name=$1
        
       file_name=`echo $File_name | tr 'A-Z' 'a-z' | sed -e 's/wf_clmrc_acre_//g' | sed -e 's/_est//g'` 
        if [ $rt_cd -ne 0 ]
        then
        echo "could not execute the command"
        fi

       tr '\\' ' ' < $TGTFILES/$file_name.dat > $TGTFILES/FF_$file_name.dat
       rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
        echo "could not execute the command"
        fi
