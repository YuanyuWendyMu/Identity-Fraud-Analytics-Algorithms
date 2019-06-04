library(dplyr)
library(lubridate)
library(caret)

####Train, test, oot split
data1=read.csv('23varaibles.csv')
data1$date=as.Date(data1$date)
data<-data1[data1$date<='2016-10-31',]
oot<-data1[data1$date>'2016-10-31',]

trainIndex = createDataPartition(data$fraud_label,
                                 p=0.75, list=FALSE,times=1)

train =data[trainIndex,]
test =data[-trainIndex,]

train$date <- NULL
test$date <- NULL

#logistic regression
lg.reg<-glm(fraud_label~.-record,data=train,family=binomial)
lg.pre<-predict(lg.reg,test,type='response')

#rank test based on lg.res, calculate 3% fraud
summary(lg.reg)

##train FDR
train_res<-data.frame(record=train$record,pro=lg.reg$fitted.values,Fraud=train$fraud_label)
num=floor(0.03*nrow(train_res))#2142
train_res_top<-train_res[order(-train_res$pro),][0:num,]
sum(train_res_top$Fraud)/sum(train_res$Fraud)#0.4899113-train
##test FDR
test_res<-data.frame(record=test$record,pro=lg.pre,Fraud=test$fraud_label)
num=floor(0.03*nrow(test_res))
test_res_top<-test_res[order(-test_res$pro),][0:num,]
sum(test_res_top$Fraud)/sum(test_res$Fraud)#0.5035152-test
##oot
lg.pre1<-predict(lg.reg,oot,type='response')
oot_res<-data.frame(record=oot$record,pro=lg.pre1,Fraud=oot$fraud_label)
num=floor(0.03*nrow(oot_res))
oot_res_top<-oot_res[order(-oot_res$pro),][0:num,]
sum(oot_res_top$Fraud)/sum(oot_res$Fraud)#0.466052-oot

#randomForest-wrong!!!
library(ranger)

#library(randomForest)#vector too largee
#rf<-randomForest(formula=Fraud~.-Recnum,train,importance=TRUE,mtry = 14,ntree=50)#500
#train_res_rf1<-predict(rf,test,type='prob')[,2]
##solve the memory issue
rf2<-ranger(fraud_label~.-record,train,importance = "impurity",num.trees =1000)
train_res_rf<-predict(rf2,train)
#
train_res_rf<-data.frame(Recnum=train$record,pro=train_res_rf[['predictions']],Fraud=as.numeric(train$fraud_label))
write.csv(train_res_rf, file = "train_rf.csv")
num=floor(0.03*nrow(train_res_rf))#2142
train_res_rf_top<-train_res_rf[order(-train_res_rf$pro),][0:num,]

sum(train_res_rf_top$Fraud)/sum(train_res_rf$Fraud)#0.5307095-train
#test FDR
test_res_rf<-predict(rf2,test)
#
test_res_rf<-data.frame(Recnum=test$record,pro=test_res_rf[['predictions']],Fraud=as.numeric(test$fraud_label))
write.csv(test_res_rf, file = "test_rf.csv")
num=floor(0.03*nrow(test_res_rf))#2142
test_res_rf_top<-test_res_rf[order(-test_res_rf$pro),][0:num,]

sum(test_res_rf_top$Fraud)/sum(test_res_rf$Fraud)#0.5296284
#oot FDR
oot_res_rf<-predict(rf2,oot)
#
oot_res_rf<-data.frame(Recnum=oot$record,pro=oot_res_rf[['predictions']],Fraud=as.numeric(oot$fraud_label))
write.csv(oot_res_rf, file = "oot_rf.csv")
num=floor(0.03*nrow(oot_res_rf))#2142
oot_res_rf_top<-oot_res_rf[order(-oot_res_rf$pro),][0:num,]

sum(oot_res_rf_top$Fraud)/sum(oot_res_rf$Fraud)#0.5

#boosting tree
#Gradient boosting tree cannot take factor as Y variable
library(gbm)
data$fraud_label=as.character(data$fraud_label)
oot$fraud_label=as.character(oot$fraud_label)
trainIndex = createDataPartition(data$fraud_label,
                                 p=0.75, list=FALSE,times=1)

train =data[trainIndex,]
test =data[-trainIndex,]
gra_boost<- gbm(formula = fraud_label~ .-record-date,
                    distribution ='bernoulli', 
                    data =train,
                    n.trees = 1000)
ntree_opt<- gbm.perf(object =gra_boost, 
                     method = 'OOB', 
                     oobag.curve = TRUE)
gra_boost<- gbm(formula = fraud_label~ .-record-date, 
                distribution ='bernoulli', 
                data =train,
                n.trees =ntree_opt)
gb_pre_train<- predict(object =gra_boost, 
                 newdata =train,type='response',
                 n.trees = ntree_opt)

gb_pre_test<- predict(object =gra_boost, 
                  newdata =test,type='response',
                  n.trees = ntree_opt)

gb_pre_oot<- predict(object =gra_boost, 
                      newdata =oot,type='response',
                      n.trees = ntree_opt)
#train FDR
train_res_bt<-data.frame(Recnum=train$record,pro=gb_pre_train,Fraud=as.numeric(train$fraud_label))
num=floor(0.03*nrow(train_res_bt))#2142
train_res_bt_top<-train_res_bt[order(-train_res_bt$pro),][0:num,]
sum(train_res_bt_top$Fraud)/sum(train_res_bt$Fraud)#0.5147679
#test FDR
test_res_bt<-data.frame(Recnum=test$record,pro=gb_pre_test,Fraud=as.numeric(test$fraud_label))
num=floor(0.03*nrow(test_res_bt))#2142
test_res_bt_top<-test_res_bt[order(-test_res_bt$pro),][0:num,]

sum(test_res_bt_top$Fraud)/sum(test_res_bt$Fraud)#0.5274908
#oot FDR
oot_res_bt<-data.frame(Recnum=oot$record,pro=gb_pre_oot,Fraud=as.numeric(oot$fraud_label))
num=floor(0.03*nrow(oot_res_bt))#2142
oot_res_bt_top<-oot_res_bt[order(-oot_res_bt$pro),][0:num,]

sum(oot_res_bt_top$Fraud)/sum(oot_res_bt$Fraud)#0.4907795

#neural network
library(nnet)
ideal <- class.ind(train$fraud_label)
nn= nnet(train[,-c(1,2,3)],ideal, size=10,  softmax = TRUE)
#train
nn_pre_train<-predict(nn,train[,-c(1,2,3)], type="raw")[,2]
train_res_nn<-data.frame(Recnum=train$record,pro=nn_pre_train,Fraud=as.numeric(train$fraud_label))
num=floor(0.03*nrow(train_res_nn))#2142
train_res_nn_top<-train_res_nn[order(-train_res_nn$pro),][0:num,]
sum(train_res_nn_top$Fraud)/sum(train_res_nn$Fraud)#0.5048856
#test
nn_pre_test<-predict(nn,test[,-c(1,2,3)], type="raw")[,2]
test_res_nn<-data.frame(Recnum=test$record,pro=nn_pre_test,Fraud=as.numeric(test$fraud_label))
num=floor(0.03*nrow(test_res_nn))#2142
test_res_nn_top<-test_res_nn[order(-test_res_nn$pro),][0:num,]
sum(test_res_nn_top$Fraud)/sum(test_res_nn$Fraud)#0.5151616
#oot
nn_pre_oot<-predict(nn,oot[,-c(1,2,3)], type="raw")[,2]
oot_res_nn<-data.frame(Recnum=oot$record,pro=nn_pre_oot,Fraud=as.numeric(oot$fraud_label))
num=floor(0.03*nrow(oot_res_nn))#2142
oot_res_nn_top<-oot_res_nn[order(-oot_res_nn$pro),][0:num,]
sum(oot_res_nn_top$Fraud)/sum(oot_res_nn$Fraud)#0.4786253

