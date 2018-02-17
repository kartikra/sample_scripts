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

	if [ ! -d "$FLAGDIR" ]
	then
		echo "FLAGDIR is missing in user folder. Create the directory and try again. Exiting Script ..."
		exit 911
	fi


# STEP-2 Cleanup the directories

	TDRegion=$1
	NumOfExecutions=$2
	fileDir=$3
	singleFile=$4
	outFile=$5
	logFile=$6
	  
	
	inputFile=`echo $singleFile | sed -e 's/\./_/g'`


	if [ ! -d $TEMPDIR/"$inputFile" ]
	then
		mkdir $TEMPDIR/"$inputFile"
	fi


	if [ ! -d $LOGDIR/"$inputFile" ]
	then
		mkdir $LOGDIR/"$inputFile"
	fi


	if [ ! -d $FLAGDIR/"$inputFile" ]
	then
		mkdir $FLAGDIR/"$inputFile"
	fi

	rm -f $TEMPDIR/"$inputFile"/*.*
	rm -f $LOGDIR/"$inputFile"/*.*
	rm -f $FLAGDIR/"$inputFile"/*.*


	fileCount="0"
	counter="0"

	totalNumber=`cat $fileDir/$singleFile | wc -l`
	parallelCount=$(($totalNumber/$NumOfExecutions))
	if [ $parallelCount -lt 1 ]
	then
		parallelCount=$totalNumber
	fi

	
# STEP-3

	# Divide Scripts in batches of $parallelCount

	rm -f $TEMPDIR/temp_"$inputFile"
	
	# Check if block level compression is enabled
	blockInd=`head -1 "$fileDir"/"$singleFile"  | grep -i "BlockCompression=YES" | wc -l`
	
	cat "$fileDir"/"$singleFile" | while read -r line ; do

		sline1=`echo $line | cut -f1 -d';'`
		sline2=`echo $line | cut -f2 -d';'`
		sline3=`echo $line | cut -f3 -d';'`
		sline4=`echo $line | cut -f4 -d';'`

		
		if [ ! -z "$sline1" ]
		then
			echo "$sline1;" >> $TEMPDIR/temp_"$inputFile"
		fi
		if [ ! -z "$sline2" ]
		then
			echo "$sline2;" >> $TEMPDIR/temp_"$inputFile"
		fi
		if [ ! -z "$sline3" ]
		then
			echo "$sline3;" >> $TEMPDIR/temp_"$inputFile"
		fi
		if [ ! -z "$sline4" ]
		then
			echo "$sline4;" >> $TEMPDIR/temp_"$inputFile"
		fi
		if [ ! -z "$sline5" ]
		then
			echo "$sline5;" >> $TEMPDIR/temp_"$inputFile"
		fi
		
		counter=`expr "$counter" + 1`
				
		if [ $counter -eq $parallelCount ]
		then
			fileCount=`expr $fileCount + 1`
			counter="0"

			dateforfile=`date +%Y%m%d%H%M%S`
			perl -pi -e 's/\\//gi'  $TEMPDIR/temp_"$inputFile" 
			mv $TEMPDIR/temp_"$inputFile" $TEMPDIR/"$inputFile"/${dateforfile}.sql
			nohup $SCRIPTDIR/epdba_runSQLFile.sh "$TDRegion" $TEMPDIR/"$inputFile"/${dateforfile}.sql $TEMPDIR/"$inputFile"/${dateforfile}.out $FLAGDIR/"$inputFile"/${dateforfile}.flag  | tee -a $LOGDIR/"$inputFile"/${dateforfile}.log &
			sleep 1

			if [ $blockInd -eq 1 ]
			then
				echo "SET QUERY_BAND='BlockCompression=YES;' FOR SESSION;" > $TEMPDIR/temp_"$inputFile"
			fi
		fi
			
		
		
	done



	# Final Set of Scripts

	if [ $counter -lt $parallelCount ] && [ $counter -ne 0 ]
	then
		fileCount=`expr $fileCount + 1`
		counter="0"
		
		dateforfile=`date +%Y%m%d%H%M%S`
		perl -pi -e 's/\\//gi'  $TEMPDIR/temp_"$inputFile" 
		
		mv $TEMPDIR/temp_"$inputFile" $TEMPDIR/"$inputFile"/${dateforfile}.sql
		nohup $SCRIPTDIR/epdba_runSQLFile.sh "$TDRegion" $TEMPDIR/"$inputFile"/${dateforfile}.sql $TEMPDIR/"$inputFile"/${dateforfile}.out $FLAGDIR/"$inputFile"/${dateforfile}.flag | tee -a $LOGDIR/"$inputFile"/${dateforfile}.log &
		sleep 1

	fi


	exeCount=`ls -l $FLAGDIR/"$inputFile" | wc -l`
	while [ $exeCount -ne $fileCount ]
	do
		sleep 5
		exeCount=`ls $FLAGDIR/"$inputFile" | wc -l`
	done

	if [ -f  $TEMPDIR/"$inputFile"/*.out ]
	then
		cat $TEMPDIR/"$inputFile"/*.out >> $outFile
		cat $TEMPDIR/"$inputFile"/*.out > $TEMPDIR/"$inputFile".out
	fi
	if [ -f  $LOGDIR/"$inputFile"/*.log ]
	then
		cat $LOGDIR/"$inputFile"/*.log >> $logFile
	fi
	
	rm -f $TEMPDIR/"$inputFile"/*.*
	rm -f $LOGDIR/"$inputFile"/*.*
	rm -f $FLAGDIR/"$inputFile"/*.*
	rmdir $TEMPDIR/"$inputFile"
	rmdir $LOGDIR/"$inputFile"
	rmdir $FLAGDIR/"$inputFile"

