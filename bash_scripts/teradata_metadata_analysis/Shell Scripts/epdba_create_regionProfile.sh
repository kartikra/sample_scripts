#!/usr/bin/ksh

# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi
		
	rm -f $SQLDIR/accdba_create_regionProfile.sql
	cat $SCRIPTDIR/accdba_env_profile.dat | cut -f1,4 -d '|' | sort | uniq | while read -r line ; do
		envType=`echo $line | cut -f1 -d'|'`
		envName=`echo $line | cut -f2 -d'|'`
		
		echo " .EXPORT RESET;" >>  $SQLDIR/accdba_create_regionProfile.sql
		echo " .EXPORT REPORT FILE = $HOMEDIR/region/$envName.profile" >> $SQLDIR/accdba_create_regionProfile.sql
		echo " EXEC CLARITY_DBA_MAINT.CLARITY_UPG_GET_ENV_DETAILS ('$envType','$envName'); " >> $SQLDIR/accdba_create_regionProfile.sql
	done
	
	if [ -f $HOMEDIR/region/*.profile ]
	then
		mv $HOMEDIR/region/*.profile $HOME/dbmig/region
	fi
	rm -f $HOMEDIR/region/*.profile $HOME/dbmig/region
	$SCRIPTDIR/epdba_runSQLFile2.sh tdd1.kp.org $SQLDIR/accdba_create_regionProfile.sql $OUTDIR/accdba_create_regionProfile.sql | tee -a  $LOGDIR/accdba_create_regionProfile.sql
	chmod 777 $HOMEDIR/region/*.profile
	
