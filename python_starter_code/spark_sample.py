1) I have generated a sequence file with tokens (uni, bi and trigrams post pre-processing) in location hdfs:///bdpilot3_h/text_mining/sequence_chp_input (5.4 mn records)
2) Please execute code until generating the top 1000 features and write them to a file. Feel free to use the SVM code as well in case you want to train a model + CV.
3) Given the data size ~5.5 mn rows, please be patient till the tf and idf vectors generate
4) In case of any errors related to memory, increase number of executors in the pyspark command line OR tweak the memory parameters in the command line.


'
create external table bdpilot3.chp_unnecessaryer_table30_final_ext
(comments string, un_er int) 
row format delimited fields terminated by '~' lines terminated by '\n'
location'/bdpilot3_h/external_table/chp_unnecessaryer_table30_final_ext';

insert overwrite table bdpilot3.chp_unnecessaryer_table30_final_ext
select regexp_replace(comments, "~", " "), un_er from bdpilot3.unnecessaryer_table30_final where comments is not null and trim(comments) not like ''

'
-----------------------------------------------------------------------------------------------------------
#export YARN_CONF_DIR=/etc/hadoop/conf
#export HADOOP_CONF_DIR=/etc/hadoop/conf
#pyspark --master yarn --py-files pre_processing.py,classes.py,validation.py,rdd.py,base.py,base_f.py,splearn_custom.py,feature_selection.py --conf spark.driver.maxResultSize=13g --num-executors 20 --driver-memory 13G --executor-memory 8G --executor-cores 2 --properties-file spark-defaults.conf 

#STart code here

import pre_processing as pp
import operator
import numpy as np
from csv import writer as csvw
from scipy.sparse import csr_matrix
from scipy.stats import norm
from splearn_custom import SparkCountVectorizer, SparkTfidfTransformer,SparkHashingVectorizer
from splearn.pipeline import SparkPipeline
from sklearn import metrics
from scipy.stats import itemfreq
from classes import SparkLinearSVC
from feature_selection import SparkVarianceThreshold
from rdd import ArrayRDD, DictRDD

def pre_process_doc(doc):
        token = pp.tokenize_doc(doc)
        token = pp.rem_stopwords(token)
        token = pp.token_stemming(token)
        bigrams = pp.bigram_doc(token)
        trigrams = pp.trigram_doc(token)
        return(token + bigrams + trigrams)

def tokenize_pre_process(token):
        return(token.split('||'))

def InfoGain(Z):
    tp = Z.map(lambda(x,y): csr_matrix(y)*x).reduce(operator.add)
    tp = tp.todense()
    fp = Z.map(lambda(x,y): csr_matrix(1-y)*x).reduce(operator.add)
    fp = fp.todense()
    psum = Z[:,'y'].sum() 
    fn = psum  - tp
    count = Z[:,'y'].shape[0]
    nsum = count - psum 
    tn = nsum - fp
    pword = (tp + fp)/count
    pnword = 1 - pword
    pos = tp + fn
    neg = tn + fp
    return(eee(pos,neg) - (np.multiply(pword, eee(tp,fp)) + np.multiply(pnword , eee(fn,tn))))

def eee(x,y):
    return ( np.multiply( (-x/(x+y)), np.log2(x/(x+y)) ) + np.multiply( (-y/(x+y)), np.log2(y/(x+y)) ) )


def BnS(Z):
    tp = Z.map(lambda(x,y): csr_matrix(y)*x).reduce(operator.add)
    tp = tp.todense()
    fp = Z.map(lambda(x,y): csr_matrix(1-y)*x).reduce(operator.add)
    fp = fp.todense()
    psum = Z[:,'y'].sum() 
    fn = psum  - tp
    nsum = Z[:,'y'].shape[0] - psum 
    tn = nsum - fp
    tpr = tp/(tp+fn)
    fpr = fp/(tn+fp)
    return((1/norm.cdf(tpr))-(1/norm.cdf(fpr)))


#Already generated no need to generate again
#rawTextRdd=sc.textFile("/bdpilot3_h/external_table/chp_unnecessaryer_table30_final_ext")
#comments_target_split_rdd = rawTextRdd.map(lambda(x): x.split("~"))
#comments_processed_rdd = comments_target_split_rdd.map(lambda(c,y): (("||".join(pre_process_doc(c)),y)))
#comments_processed_rdd.saveAsSequenceFile("hdfs:///bdpilot3_h/text_mining/sequence_chp_input")

new_cleanedRdd=sc.sequenceFile(path="hdfs:///bdpilot3_h/text_mining/sequence_chp_input",minSplits=500)
new_train_rdd,new_test_rdd = new_cleanedRdd.randomSplit([0.7,0.3])

new_train_z = DictRDD(new_train_rdd, columns=('X','y'),bsize=2000)
new_test_z = DictRDD(new_test_rdd, columns=('X','y'),bsize=2000)

#Computing binary DTM
new_countvectorizer = SparkCountVectorizer(tokenizer=tokenize_pre_process, binary=True,dtype=float)
new_train_count = new_countvectorizer.fit_transform(new_train_z)
new_test_count = new_countvectorizer.transform(new_test_z)
new_allFeatures = new_countvectorizer.vocabulary_
new_swapFeatures = dict((v,t) for t,v in new_allFeatures.iteritems())

#Computing tf-idf DTM
dist_pipeline = SparkPipeline((
    ('vect', SparkCountVectorizer(tokenizer=tokenize_pre_process)),
    ('tfidf', SparkTfidfTransformer(smooth_idf=True))
))

new_train_tfidf = dist_pipeline.fit_transform(new_train_z)
new_test_tfidf = dist_pipeline.transform(new_test_z)

#Computing feature selection scores
new_ig = InfoGain(new_train_count)
new_ig = np.array(new_ig)
new_ig = new_ig[0]
new_ig[np.isnan(new_ig)] = -999999
new_bns = BnS(train_count)
new_bns = new_bns[0]

#Selecting top n words
n = 1000
top_n_bns = [swapFeatures[i] for i in bns.argsort()[-n:][::-1]]
top_n_ig = [swapFeatures[i] for i in ig.argsort()[-n:][::-1]]
top_n_vt = [swapFeatures[i] for i in vt]

#Writing out the top words
with open('top_BnS_words.csv','wt') as f:
     writer = csvw(f)
     for i in top_n_bns:
         writer.writerow([i])

with open('top_IG_words.csv','wt') as f:
     writer = csvw(f)
     for i in top_n_ig:
         writer.writerow([i])

#############################################################################
#Transform DTM based on InfoGain
############################################################################
new_train_tfidf_ig = new_train_tfidf.map(lambda(x,y): (x[:,new_ig.argsort()[-k:]],y))
new_test_tfidf_ig = new_test_tfidf.map(lambda(x,y): (x[:,new_ig.argsort()[-k:]],y))
new_train_tfidf_ig.first()
new_test_tfidf_ig.first()
new_train_Z = DictRDD(new_train_tfidf_ig, columns=('X','y'), bsize=7500)
new_test_Z = DictRDD(new_test_tfidf_ig, columns=('X','y'), bsize=7500)

#Transform DTM based on BnS
#train_tfidf_bns = train_tfidf.map(lambda(x,y): (x[:,bns.argsort()[-k:][::-1]],y))
#test_tfidf_bns = test_tfidf.map(lambda(x,y): (x[:,bns.argsort()[-k:][::-1]],y))
#train_tfidf_bns.first()
#test_tfidf_bns.first()
#train_Z = DictRDD(train_tfidf_bns, columns=('X','y'), bsize=2000)
#test_Z = DictRDD(test_tfidf_bns, columns=('X','y'), bsize=2000)

#new_train_y = new_train_Z[:,'y'].toarray()

#Train the SVM model
svm_model = SparkLinearSVC(class_weight='auto',C=0.1)
svm_model.fit(new_train_Z,classes=np.array([0,1]))

#Predict for the test set
new_predicted_y = svm_model.predict(new_test_Z[:,'X'])
new_predicted_y = new_predicted_y.toarray()
new_true_y = new_test_Z[:,'y'].toarray()


#Computing metrics
cm = metrics.confusion_matrix(new_true_y,new_predicted_y)
itemfreq(new_predicted_y)
itemfreq(new_true_y)
cm
rec = (1.0*cm[1,1])/(cm[1,0]+cm[1,1])
pr = (1.0*cm[1,1])/(cm[0,1]+cm[1,1])
f1 = 2 *((pr*rec)/(pr+rec)) 
acc = (1.0*(cm[1,1]+cm[0,0])/len(new_true_y))
print pr,rec,f1,acc


###Cross validation

#tot = new_train_Z._rdd.union(new_test_Z)
#cv_train, cv_test = tot.randomSplit([0.7,0.3])
#cv_train_Z = DictRDD(cv_train, columns=('X','y'), bsize=7500)
#cv_test_Z = DictRDD(cv_test, columns=('X','y'), bsize=7500)
#cv_train_y = cv_train_Z[:,'y'].toarray()

#svm_model = SparkLinearSVC(class_weight='auto',C=0.1)
#svm_model.fit(cv_train_Z,classes=np.array([0,1]))

#Predict for the test set
new_predicted_y = svm_model.predict(cv_test_Z[:,'X'])
new_predicted_y = new_predicted_y.toarray()
new_true_y = new_test_Z[:,'y'].toarray()

#Computing metrics
cm = metrics.confusion_matrix(new_true_y,new_predicted_y)
itemfreq(new_predicted_y)
itemfreq(new_true_y)
cm
rec = (1.0*cm[1,1])/(cm[1,0]+cm[1,1])
pr = (1.0*cm[1,1])/(cm[0,1]+cm[1,1])
f1 = 2 *((pr*rec)/(pr+rec)) 
acc = (1.0*(cm[1,1]+cm[0,0])/len(new_true_y))
print pr,rec,f1,acc

###############################Prediction at member week level###########################

#mbrweek_new_rdd=sc.textFile("hdfs:///bdpilot3_h/external_table/prediction_master_data_new_ext")
#comments_mbrweek_target_split_rdd = mbrweek_new_rdd.map(lambda(x): x.split("~"))
#comments_mbrweek_new_processed_rdd = comments_mbrweek_target_split_rdd.map(lambda(m,d,c): ("~".join([m,d]),("||".join(pre_process_doc(c)))))
#comments_mbrweek_new_processed_rdd.saveAsSequenceFile("hdfs:///bdpilot3_h/text_mining/sequence_mbrweek_new")

cleaned_mbrweek_new_rdd =sc.sequenceFile(path="hdfs:///bdpilot3_h/text_mining/sequence_mbrweek_new",minSplits=500)

mbrweek_new_x = DictRDD(cleaned_mbrweek_new_rdd, columns=('y','X'),bsize=100)
mbrweek_new_tfidf = dist_pipeline.transform(mbrweek_new_x)
mbrweek_new_tfidf_ig = mbrweek_new_tfidf.map(lambda(y,x): (y,x[:,new_ig.argsort()[-k:]]))
mbrweek_new_Z = DictRDD(mbrweek_new_tfidf_ig, columns=('y','X'),bsize=100)
predicted_new_mbrweek = svm_model.predict(mbrweek_new_Z[:,'X'])
predicted_new_mbrweek._rdd.saveAsTextFile("hdfs:///bdpilot3_h/text_mining/results/new/predictions.csv")
aa=mbrweek_new_Z[:,"X'].map(lambda x : svm_model.decision_function(x))
aa._rdd.saveAsTextFile("hdfs:///bdpilot3_h/text_mining/results/new/confidence_score.csv")
mbrweek_new_Z[:,'y']._rdd.saveAsTextFile("hdfs:///bdpilot3_h/text_mining/results/new/member_week.csv")

cat * > result.txt
sed 's/u''/\n/g' member_week.csv >> test.csv
sed -e '/^ *$/d' test.csv >> test.csv
sed 's/[/\n/g' test.csv > test.csv

predicted_new_mbrweek = predicted_new_mbrweek.toarray()

#Selecting top n words
n = 1000
top_n_bns = [swapFeatures[i] for i in bns.argsort()[-n:][::-1]]
top_n_ig = [swapFeatures[i] for i in ig.argsort()[-n:][::-1]]
top_n_vt = [swapFeatures[i] for i in vt]

#Writing out the top words
with open('top_BnS_words.csv','wt') as f:
     writer = csvw(f)
     for i in top_n_bns:
         writer.writerow([i])

with open('top_IG_words.csv','wt') as f:
     writer = csvw(f)
     for i in top_n_ig:
         writer.writerow([i])





