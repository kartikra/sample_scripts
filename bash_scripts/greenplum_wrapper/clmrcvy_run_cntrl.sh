#!/usr/bin/ksh
#---------------------- Start of Script clmrcvy_run_cntrl.sh ------------------------------------#

#-------------------------------------- START MAIN SCRIPT ---------------------------------------------#
#CODE="/gpfs01/dev/edl/code"
#PMDIR="/gpfs01/dev/edl/pcenterdata"

# STEP-1 Read Input Parameters


        if [ $1 == "S" ]
        then
               jobTypCd=$2
               jobCode=$3
               runDesc=$4
               infaRunId=$5
        else
               jobCode=$2
               jobStatus=$3
               runDesc=$4
               src_count=$5
               trgt_count=$6
               err_count=$7
               trgt_table_name=$8
               trgt_table_date=$9
               refStrtDt=${10}
               refEndDt=${11}
            

############################################################################################
#CHANGED ON 23 DEc BY SUHAIL QADIR BEIG:TO PROVIDE THE SOURCE CODE FOR JOB CODE TYPE 6.....
############################################################################################
echo $runDesc | grep FACETS
            rt_cd=$?
            if [ $rt_cd -eq 0 ]
            then
            src_Code=823
            fi

            echo $runDesc | grep STAR
            rt_cd=$?
            if [ $rt_cd -eq 0 ]
            then
            src_Code=815
            fi


            echo $runDesc | grep WGS
            rt_cd=$?
            if [ $rt_cd -eq 0 ]
            then
            src_Code=808
            fi

            echo $runDesc | grep CS90
            rt_cd=$?
            if [ $rt_cd -eq 0 ]
            then
            src_Code=809
            fi 
        fi


#STEP-2 Run Profile File

        USR_PROF=$CODE/clmrcvy_gp/scripts/clmrcvy.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then  
                echo "Profile file cannot be found, Exiting" >> $logFileName 2>&1
                exit 902
        fi


        
#STEP-3 Create Log File for current run on start of job. If job has already started get the log file from run_cntrl table

        shellName=`basename $0`
        if [ $1 == "S" ]
        then

                today=`date +"%Y_%m_%d_%H_%M_%S"`
                
                runDesc=`echo "$runDesc" | sed -e's/ERROR/ERR/g'`
#################################################################################################
#CHANGED ON 23 DEc BY SUHAIL QADIR BEIG: TRANSLATE WAS NOT WORKING IN SOME CASES SO HAD TO MAKE
# MADE CHANGES TO tr [A-Z] [a-z] and made it tr 'A-Z' 'a-z'.........
#################################################################################################

                logFile=`echo "$runDesc $today"  | tr 'A-Z' 'a-z' | sed -e 's/ /_/g'`
                logFileName=$LOGDIR/$logFile.log
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
                
        
        else
                rm -f $SQLDIR/$runDesc.sql
                rm -f $SQLDIR/$runDesc.dat


                echo "SELECT MAX(job_typ_cd) || '|' || MAX(log_file_nm) FROM run_cntrl WHERE job_cd='$jobCode' AND run_stts='STARTED'" > $SQLDIR/$runDesc.sql
                $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f $runDesc.sql -l $SQLDIR/$runDesc.dat  
                rt_cd=$?
                if [ $rt_cd -ne 0 ]
                then
                       echo "ERROR - Unable to Run SQL File" > $SQLDIR/$runDesc.dat
                       exit 101
                fi
                currData=`cat $SQLDIR/$runDesc.dat | head -2 | tail -1 | sed 's/ //g'`
                currLogFile=`echo "$currData" | cut -f2 -d '|'`

                jobTypCd=`echo "$currData" | cut -f1 -d '|'`

                logFileName=$LOGDIR/$currLogFile

                rm -f $SQLDIR/$runDesc.sql
                rm -f $SQLDIR/$runDesc.dat


		# SET The Appropriate Value for target table

		schema_name=`echo $trgt_table_name | cut -f1 -d'.'`
		table_name=`echo $trgt_table_name | cut -f2 -d'.'`
		
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
		
		trgt_table=$schema_name.$table_name
		echo "Target Table is $trgt_table" >> $logFileName

        fi


        user_machine=`who -m | cut -f2 -d'(' | sed 's/)//g'` >> $logFileName
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "ERROR - Unable to Get user_machine" >> $logFileName
                err_cd=801
                exit 801
        fi
        if [ -z "$user_machine" ]
        then
               user_machine="BATCH"
        fi



#STEP-4 Create Entry for current run on start of job


        if [ $1 == "S" ]
        then
               fieldList="run_cntrl_id,job_cd,job_typ_cd,strt_tm,end_tm,run_stts, \
                           creatn_dt,creatd_by,updt_dt,updtd_by,updt_host, \
                           run_desc,log_file_nm, infa_run_id"
                           
               fieldValueList="nextval('seq_run_cntrl_id'),'$jobCode',$jobTypCd, \
                                current_timestamp,current_timestamp,'STARTED',current_timestamp, \
                                current_user,current_timestamp,user,'$user_machine', \
                                '$runDesc','$logFile.log', $infaRunId"

               echo "INSERT INTO run_cntrl($fieldList) VALUES($fieldValueList)" > $SQLDIR/$runDesc.sql


               $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f $runDesc.sql -l $logFileName  
               rt_cd=$?
               if [ $rt_cd -ne 0 ]
               then
                      echo "ERROR - Unable to Run SQL File" >> $logFileName
                      exit 101
               fi

               rm -f $SQLDIR/$runDesc.sql

        else

                
                
                # STEP-5 Get target count from greenplum
                # If the target is a file job typ code will be 3. Do not take counts from greenplum if target is a file

                #---------------------------------------------------------------------------#

                rm -f $SQLDIR/$runDesc-audit.sql
                rm -f $SQLDIR/$runDesc-audit.dat
                


                if [ "$jobTypCd" != "3" ] 
                then
#################################################################################################
#CHANGED ON 23 DEc BY SUHAIL QADIR BEIG:MADE CHANGES TO THIS CONDITION SO THAT THE AUDIT FOR FULL
#REFRESH DOESNT GET PICKED FOR THE JOB TYPE CODE 6 WHERE SOURCE SODE IS MENTIONED......
#################################################################################################

	            if [ ! -z "$src_Code" ] && [ "$jobTypCd" == "6" ] && [ -z "$trgt_table_date" ]
                    then

                       auditSql="SELECT COUNT(*) FROM $trgt_table where \
                               mbrshp_sor_cd = '$src_Code'"
		
                    else

                        if [ -z "$trgt_table_date" ]
                        then
                               auditSql="SELECT COUNT(*) FROM $trgt_table"

                        else

                                CURMTH=`echo $(date '+%m')` 
                                CURYR=`echo $(date '+%y')`  
                                        
                                # Run Date shoul be set to first day and last day 
                                # of previous month
                
                                if [ $CURMTH -eq 1 ]
                                then 
                                        PRVMTH=12
                                        PRVYR=`expr 2000 + $CURYR - 1`
                                else 
                                        PRVMTH=`expr $CURMTH - 1`
                                        PRVYR=`expr 2000 + $CURYR`
                                fi

                                if [ $PRVMTH -lt 10 ]
                                then 
                                        PRVMTH=`echo "0$PRVMTH"`
                                fi
                                LASTDY=`cal $PRVMTH $PRVYR | egrep "28|29|30|31" |tail -1 |awk '{print $NF}'`  2>> $logFileName
                                newStartDate="$PRVYR-$PRVMTH-01"
                                newEndDate="$PRVYR-$PRVMTH-$LASTDY"
                                
                                if [ -z "$refStrtDt" ]
                                then
                                       refStrtDt=$newStartDate
                                fi
                                if [ -z "$refEndDt" ]
                                then
                                       refEndDt=$newEndDate
                                fi


                               auditSql="SELECT COUNT(*) FROM $trgt_table  \
                                           WHERE $trgt_table_date BETWEEN \
                                            CAST('$refStrtDt' AS DATE) AND CAST('$refEndDt' AS DATE)" 
                        
                        fi

                    fi

                        echo $auditSql >> $logFileName
                        echo $auditSql > $SQLDIR/$runDesc-audit.sql

                        $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f $runDesc-audit.sql -l $SQLDIR/$runDesc-audit.dat  
                        rt_cd=$?
                        if [ $rt_cd -ne 0 ]
                        then
                               echo "ERROR - Unable to Run SQL File" >> $logFileName
                               exit 101
                        fi

                        gpTrgtCnt=`cat $SQLDIR/$runDesc-audit.dat | head -2 | tail -1 | sed 's/ //g'`

                        if [[ $gpTrgtCnt = +([0-9]) ]]
                        then
                                echo "Target Count from GP for $trgt_table is $gpTrgtCnt" >> $logFileName
                        else
                                gpTrgtCnt=0
                        fi

                        rm -f $SQLDIR/$runDesc-audit.sql
                        rm -f $SQLDIR/$runDesc-audit.dat

                        #---------------------------------------------------------------------------#

                        if [ -z "$refStrtDt" ]
                        then
                               refStrtDt="9999-12-31"
                        fi
                        if [ -z "$refEndDt" ]
                        then
                               refEndDt="9999-12-31"
                        fi

                else
                        gpTrgtCnt=$trgt_count
                        if [ -z "$refStrtDt" ]
                        then
                               refStrtDt="9999-12-31"
                        fi
                        if [ -z "$refEndDt" ]
                        then
                               refEndDt="9999-12-31"
                        fi
                fi
                

                if [ -f "$LOGDIR/$runDesc.log" ] && [ "$jobTypCd" == "6" ]
                then

                        cat $LOGDIR/$runDesc.log | grep -i "|INFO|rows Inserted" | cut -f2 -d'=' | sed 's/ //g' > $TEMPDIR/rowswritten-$today.dat
                        echo `sed 's/$/+/' $TEMPDIR/rowswritten-$today.dat` 0 | bc >> $logFileName 2>&1
                        row_write=`cat $logFileName | tail -1`

                        cat $LOGDIR/$runDesc.log | grep -i "|WARN|" | cut -f3 -d'|' | cut -f1 -d' ' > $TEMPDIR/rowserror-$today.dat
                        echo `sed 's/$/+/' $TEMPDIR/rowserror-$today.dat` 0 | bc >> $logFileName 2>&1
                        err_count=`cat $logFileName | tail -1`

                        err_count=`expr $err_count + $7`
                        
                        rm -f $TEMPDIR/rowswritten-$today.dat
                        rm -f $TEMPDIR/rowserror-$today.dat

                else
                        row_write=$gpTrgtCnt
                fi

                load_count=`expr $src_count - $err_count`

                if [ "$load_count" != "$gpTrgtCnt" ]
                then
                        jobStatus='WARNING'
                        err_desc="WARNING : Greenplum Target Count does not match Extracted Target Count"
                        echo $err_desc >> $logFileName 
                fi


                # STEP-6  Update Entry for current run on end of job

                sql_command="UPDATE run_cntrl SET \
                src_rows_read=$src_count, \
                src_rows_procsd=$trgt_count, \
                trgt_success_cnt=$gpTrgtCnt, \
                trgt_err_cnt=$err_count, \
                trgt_insrtd_rows=$row_write, \
                trgt_updtd_rows=0, \
                trgt_deleted_rows=0, \
                run_stts='$jobStatus',   \
                err_desc='$err_desc',    \
                ref_strt_dt=CAST('$refStrtDt' AS DATE), \
                ref_end_dt=CAST('$refEndDt' AS DATE),  \
                end_tm=current_timestamp,updtd_by=current_user,  \
                updt_host='$user_machine', \
                updt_dt=current_timestamp \
                WHERE job_cd='$jobCode'  AND run_stts='STARTED'  \
                  AND log_file_nm='$currLogFile' " 
                
                echo $sql_command > $SQLDIR/$runDesc.sql
                

                $SCRIPTSDIR/run_sqlFile.sh -d $SQLDIR -f $runDesc.sql -l $logFileName  
                rt_cd=$?
                if [ $rt_cd -ne 0 ]
                then
                       echo "ERROR - Unable to Run SQL File" >> $logFileName
                       exit 101
                fi

                rm -f $SQLDIR/$runDesc.sql

        fi


        

#STEP-7 Update the final status if end of run

        if [ $1 == "E" ]
        then

                rm -f $SQLDIR/$runDesc-smmry
                command="SELECT strt_tm, end_tm, src_rows_read,src_rows_procsd, trgt_success_cnt, trgt_err_cnt,run_stts \
                          FROM run_cntrl \
                          WHERE job_cd='$jobCode'  AND log_file_nm='$currLogFile' " 


                echo  "SET SEARCH_PATH=$GPSTGSCHEMA,$GPRPTSCHEMA;" > $SQLDIR/$runDesc-smmry.sql 

		if [ "$jobTypCd" != "3" ] && [ "$table_name" != "edward_clm_lz" ]  && [ "$table_name" != "edward_wgs_mbr_dtl_lz" ]  && [ "$table_name" != "edward_wgs_mbr_prod_enrlmnt_coa_lz" ]  && [ "$table_name" != "edward_mbr_dtl_lz" ]  && [ "$table_name" != "edward_mbr_prod_enrlmnt_coa_lz" ]
		then
			echo "ANALYZE $trgt_table;" >> $SQLDIR/$runDesc-smmry.sql
		fi

		echo "JOBCODE : $jobCode"  >> $logFileName

                echo $command >> $SQLDIR/$runDesc-smmry.sql
                psql -h $GPHOST -U $GPUSER -d $GPDB -f $SQLDIR/$runDesc-smmry.sql  >> $logFileName 

                rt_cd=$?
                if [ $rt_cd -ne 0 ]
                then
                       echo "ERROR - Unable to Run SQL File" >> $logFileName
                       exit 101
                fi
                rm -f $SQLDIR/$runDesc-smmry.sql

                echo "**************************************************************************************" >> $logFileName 2>&1
                echo "Program $shellName Ended on: `date '+%m/%d/%Y %T'`" >> $logFileName 2>&1
                echo "**************************************************************************************" >> $logFileName 2>&1

                `sed 's/$//g' $logFileName > $TEMPDIR/mail.dat`
                uuencode $TEMPDIR/mail.dat $runDesc.log |  \
                mailx -s "JOB $jobCode ended with $jobStatus in $GP_ENV. Workflow Description - $runDesc " \
                "dl-BABW-Claims-Accuracy-Tech@mycompany.com" 

        fi

#------------------------ End of Script clmrcvy_create_ctrl_file.sh ---------------------------------------#
