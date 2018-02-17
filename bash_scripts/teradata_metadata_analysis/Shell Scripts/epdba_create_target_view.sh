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
		
		
	in_file=$1
	out_file=$2
	ticketNo=$3
	migReplaceList=$4
	suffix=$5
	
	rm -f $TEMPDIR/get_errors.tmp
	
# STEP-2 Create the target view defintion by replacing
#       1.   CREATE VIEW with REPLACE VIEW
#       2.   Source Database Name with Target Database Names
#       3.   Additional Repalcements 
#       4.   Cleanup old ticket Numbers and Add the current ticket number
	
	cp $in_file $TEMPDIR/new_view"$suffix".sql
	
	perl -pi -e 's/'CREATE\ *VIEW'/'REPLACE\ VIEW'/gi'  $TEMPDIR/new_view"$suffix".sql 
	perl -pi -e 's/'CV\ *HCCL'/'REPLACE\ VIEW\ HCCL'/gi'  $TEMPDIR/new_view"$suffix".sql 
	perl -pi -e 's/'CV\ *\"*HCCL'/'REPLACE\ VIEW\ \"HCCL'/gi'  $TEMPDIR/new_view"$suffix".sql 
	perl -pi -e 's/'CV\ *KPBI'/'REPLACE\ VIEW\ KPBI'/gi'  $TEMPDIR/new_view"$suffix".sql 
	perl -pi -e 's/'CV\ *\"*KPBI'/'REPLACE\ VIEW\ \"KPBI'/gi'  $TEMPDIR/new_view"$suffix".sql 
	
	if [ ! -z $migReplaceList ]
	then
		if [ -f $migReplaceList ]
		then
		
			# Start Replacing the Databasenames
			cat $migReplaceList | while read -r chgList ; do
				srcObject=`echo $chgList | cut -f1 -d '|'`
				tgtObject=`echo $chgList | cut -f2 -d '|'`
				objType=`echo $chgList | cut -f3 -d '|'`


				if [ "$objType" == "V" ]
				then
					# grep -i -n  "$srcObject" $TEMPDIR/new_view"$suffix".sql > $TEMPDIR/get_cols.tmp
					# cat $TEMPDIR/get_cols.tmp  | cut -f1 -d':' | sort | uniq | while read -r lineNumber ; do
							# sed  ''$lineNumber's/\"//g'  $TEMPDIR/new_view"$suffix".sql > $TEMPDIR/new_view.tmp
							# mv $TEMPDIR/new_view.tmp $TEMPDIR/new_view"$suffix".sql
					# done
					
					# Remove Tab or Space after Source Database Name
					perl -pi -e 's/'$srcObject'\ *\./'$srcObject'\./gi'  $TEMPDIR/new_view"$suffix".sql
					perl -pi -e 's/'$srcObject'\ *\"/'$srcObject'\"/gi'  $TEMPDIR/new_view"$suffix".sql
					perl -pi -e 's/'$srcObject'\./'$tgtObject'\./gi'  $TEMPDIR/new_view"$suffix".sql
					perl -pi -e 's/\"'$srcObject'\"\./'$tgtObject'\./gi'  $TEMPDIR/new_view"$suffix".sql

					
				else
					perl -pi -e 's/'$srcObject'/'$tgtObject'/gi'  $TEMPDIR/new_view"$suffix".sql 
					perl -pi -e 's/'$tgtObject'\.\ /'$tgtObject'\./gi'  $TEMPDIR/new_view"$suffix".sql 
				fi
			done
		fi
	fi
	
	
	if [ -f $TEMPDIR/new_view"$suffix".sql ]
	then
		rm -f $SQLDIR/new_view"$suffix".sql
		mv $TEMPDIR/new_view"$suffix".sql $SQLDIR
		perl -pi -e 's/\bREPLACE\ *VIEW\b/REPLACE\ VIEW\ \/\*'$ticketNo'\*\//i'  $SQLDIR/new_view"$suffix".sql
		
		
		ticketPreFix=`echo $ticketNo | awk '{print substr($0,1,3)}'`
		if [ "$ticketPreFix" == "CRQ" ]
		then
			# Cleanup Old CRQ Numbers only for Prod Upgrades
			$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR new_view"$suffix".sql
		else
			$SCRIPTDIR/epdba_cleanup_sql_scripts.sh $SQLDIR new_view"$suffix".sql N
		fi
		
		
		cat $SQLDIR/new_view"$suffix".sql > $out_file
		rm -f $SQLDIR/new_view"$suffix".sql
		
		if [ -s $TEMPDIR/get_errors.tmp ]
		then
			echo "/* UNABLE to Replace Source Object Name at the following lines - "  >> $out_file
			cat $TEMPDIR/get_errors.tmp >> $out_file
			echo "*/"  >> $out_file
		fi
		
	fi
	
	

	
