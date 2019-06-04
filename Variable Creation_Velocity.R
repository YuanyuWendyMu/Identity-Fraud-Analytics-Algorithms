##Reading the data as data table
#data = read.csv('applications_velocity.csv')
library(data.table)
library(lubridate)

data = fread('velocity_halfway.csv',stringsAsFactors = F)
data$date = ymd(data$date)
data2 = fread('applications_velocity.csv',stringsAsFactors = F)
data2$date = ymd(data2$date)
setDT(data2)
##rm(data2)

##Create Time Window
time_window = function(data, n, byVar){
  dt1 = data2
  dt1$join_ts1 = dt1$date
  dt1$join_ts2 = dt1$date + n
  dt1$join_rec = dt1$record
  keys = c(byVar, 'join_ts1 <= date', 'join_ts2 >= date', 'record <= record')
  dt2 = dt1[data2, on=keys, allow.cartesian=T]
  return(dt2)
}

##Group by single variable
for (i in c(0,1,3,7,14)){
    assign(paste0("data",'ssn',i), time_window(data2, i, 'ssn'))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",'fulladdress',i), time_window(data2, i, 'fulladdress'))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",'nameDOB',i), time_window(data2, i, 'nameDOB'))
}

for (i in c(0,1)){
  assign(paste0("data",'homephone',i), time_window(data2, i, 'homephone'))
}


##Group by two combinations
##ssn * all else
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','firstname')[1],c('ssn','firstname')[2],i), time_window(data, i,c('ssn','firstname') ))
}

for (i in c(0,1,3,7,14)){
    assign(paste0("data",c('ssn','lastname')[1],c('ssn','lastname')[2],i), time_window(data, i,c('ssn','lastname') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','address')[1],c('ssn','address')[2],i), time_window(data, i,c('ssn','address') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','zip5_str')[1],c('ssn','zip5_str')[2],i), time_window(data, i,c('ssn','zip5_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','dob_str')[1],c('ssn','dob_str')[2],i), time_window(data, i,c('ssn','dob_str') ))
}
 
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','homephone')[1],c('ssn','homephone')[2],i), time_window(data, i,c('ssn','homephone') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','nameDOB')[1],c('ssn','nameDOB')[2],i), time_window(data, i,c('ssn','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('ssn','fulladdress')[1],c('ssn','fulladdress')[2],i), time_window(data, i,c('ssn','fulladdress') ))
}

##firstname * all else

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','lastname')[1],c('firstname','lastname')[2],i), time_window(data, i,c('firstname','lastname') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','address')[1],c('firstname','address')[2],i), time_window(data, i,c('firstname','address') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','zip5_str')[1],c('firstname','zip5_str')[2],i), time_window(data, i,c('firstname','zip5_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','dob_str')[1],c('firstname','dob_str')[2],i), time_window(data, i,c('firstname','dob_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','homephone')[1],c('firstname','homephone')[2],i), time_window(data, i,c('firstname','homephone') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','nameDOB')[1],c('firstname','nameDOB')[2],i), time_window(data, i,c('firstname','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('firstname','fulladdress')[1],c('firstname','fulladdress')[2],i), time_window(data, i,c('firstname','fulladdress') ))
}

##lastname * all else

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('lastname','address')[1],c('lastname','address')[2],i), time_window(data, i,c('lastname','address') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('lastname','zip5_str')[1],c('lastname','zip5_str')[2],i), time_window(data, i,c('lastname','zip5_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('lastname','dob_str')[1],c('lastname','dob_str')[2],i), time_window(data, i,c('lastname','dob_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('lastname','homephone')[1],c('lastname','homephone')[2],i), time_window(data, i,c('lastname','homephone') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('lastname','nameDOB')[1],c('lastname','nameDOB')[2],i), time_window(data, i,c('lastname','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('lastname','fulladdress')[1],c('lastname','fulladdress')[2],i), time_window(data, i,c('lastname','fulladdress') ))
}

## address * all else
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('address','zip5_str')[1],c('address','zip5_str')[2],i), time_window(data, i,c('address','zip5_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('address','dob_str')[1],c('address','dob_str')[2],i), time_window(data, i,c('address','dob_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('address','homephone')[1],c('address','homephone')[2],i), time_window(data, i,c('address','homephone') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('address','nameDOB')[1],c('address','nameDOB')[2],i), time_window(data2, i,c('address','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('address','fulladdress')[1],c('address','fulladdress')[2],i), time_window(data2, i,c('address','fulladdress') ))
}

## zip5 * all else
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('zip5_str','dob_str')[1],c('zip5_str','dob_str')[2],i), time_window(data2, i,c('zip5_str','dob_str') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('zip5_str','homephone')[1],c('zip5_str','homephone')[2],i), time_window(data2, i,c('zip5_str','homephone') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('zip5_str','nameDOB')[1],c('zip5_str','nameDOB')[2],i), time_window(data2, i,c('zip5_str','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('zip5_str','fulladdress')[1],c('zip5_str','fulladdress')[2],i), time_window(data2, i,c('zip5_str','fulladdress') ))
}

## dob_str * all else
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('dob_str','homephone')[1],c('dob_str','homephone')[2],i), time_window(data2, i,c('dob_str','homephone') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('dob_str','nameDOB')[1],c('dob_str','nameDOB')[2],i), time_window(data2, i,c('dob_str','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('dob_str','fulladdress')[1],c('dob_str','fulladdress')[2],i), time_window(data2, i,c('dob_str','fulladdress') ))
}

##homephone * all else
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('homephone','nameDOB')[1],c('homephone','nameDOB')[2],i), time_window(data2, i,c('homephone','nameDOB') ))
}

for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('homephone','fulladdress')[1],c('homephone','fulladdress')[2],i), time_window(data2, i,c('homephone','fulladdress') ))
}

## name DOB * all esle
for (i in c(0,1,3,7,14)){
  assign(paste0("data",c('nameDOB','fulladdress')[1],c('nameDOB','fulladdress')[2],i), time_window(data2, i,c('nameDOB','fulladdress') ))
}


###Aggregrate results

##ssn

datassn0agg <- datassn0[, .(count = .N),by=record]
data$datassn0agg = datassn0agg$count

datassn1agg <- datassn1[, .(count = .N),by=record]
data$datassn1agg = datassn1agg$count

datassn3agg <- datassn3[, .(count = .N),by=record]
data$datassn3agg = datassn3agg$count

datassn7agg <- datassn7[, .(count = .N),by=record]
data$datassn7agg = datassn7agg$count

datassn14agg <- datassn14[, .(count = .N),by=record]
data$datassn14agg = datassn14agg$count

rm(datassn1,datassn0agg,datassn1agg,datassn3agg,datassn7agg,datassn14agg)

##fulladdress
datafulladdress0agg <- datafulladdress0[, .(count = .N),by=record]
data$datafulladdress0agg = datafulladdress0agg$count

datafulladdress1agg <- datafulladdress1[, .(count = .N),by=record]
data$datafulladdress1agg = datafulladdress1agg$count

datafulladdress3agg <- datafulladdress3[, .(count = .N),by=record]
data$datafulladdress3agg = datafulladdress3agg$count

datafulladdress7agg <- datafulladdress7[, .(count = .N),by=record]
data$datafulladdress7agg = datafulladdress7agg$count

datafulladdress14agg <- datafulladdress14[, .(count = .N),by=record]
data$datafulladdress14agg = datafulladdress14agg$count

rm(datafulladdress0,datafulladdress1,datafulladdress3,datafulladdress7,datafulladdress14)
rm(datafulladdress0agg,datafulladdress1agg,datafulladdress3agg,datafulladdress7agg,datafulladdress14agg)

##nameDOB
datanameDOB0agg <- datanameDOB0[, .(count = .N),by=record]
data$datanameDOB0agg = datanameDOB0agg$count

datanameDOB1agg <- datanameDOB1[, .(count = .N),by=record]
data$datanameDOB1agg = datanameDOB1agg$count

datanameDOB3agg <- datanameDOB3[, .(count = .N),by=record]
data$datanameDOB3agg = datanameDOB3agg$count

datanameDOB7agg <- datanameDOB7[, .(count = .N),by=record]
data$datanameDOB7agg = datanameDOB7agg$count

datanameDOB14agg <- datanameDOB14[, .(count = .N),by=record]
data$datanameDOB14agg = datanameDOB14agg$count

rm(datanameDOB0,datanameDOB1,datanameDOB3,datanameDOB7,datanameDOB14)
rm(datanameDOB0agg,datanameDOB1agg,datanameDOB3agg,datanameDOB7agg,datanameDOB14agg)

##homephone
datahomephone0agg <- datahomephone0[, .(count = .N),by=record]
data$datahomephone0agg = datahomephone0agg$count

datahomephone1agg <- datahomephone1[, .(count = .N),by=record]
data$datahomephone1agg = datahomephone1agg$count

#datahomephone3agg <- datahomephone3[, .(count = .N),by=record]
#data$datahomephone3agg = datahomephone3agg$count

#datahomephone7agg <- datahomephone7[, .(count = .N),by=record]
#data$datahomephone7agg = datahomephone7agg$count

#datahomephone14agg <- datahomephone14[, .(count = .N),by=record]
#data$datahomephone14agg = datahomephone14agg$count

rm(datahomephone0,datahomephone1)
rm(datahomephone0agg,datahomephone1agg)

##ssn firstname
datassnfirstname0agg <- datassnfirstname0[, .(count = .N),by=record]
data$datassnfirstname0agg = datassnfirstname0agg$count

datassnfirstname1agg <- datassnfirstname1[, .(count = .N),by=record]
data$datassnfirstname1agg = datassnfirstname1agg$count

datassnfirstname3agg <- datassnfirstname3[, .(count = .N),by=record]
data$datassnfirstname3agg = datassnfirstname3agg$count

datassnfirstname7agg <- datassnfirstname7[, .(count = .N),by=record]
data$datassnfirstname7agg = datassnfirstname7agg$count

datassnfirstname14agg <- datassnfirstname14[, .(count = .N),by=record]
data$datassnfirstname14agg = datassnfirstname14agg$count

rm(datassnfirstname0,datassnfirstname0agg,datassnfirstname1,datassnfirstname1agg,datassnfirstname3)
rm(datassnfirstname3agg,datassnfirstname7,datassnfirstname7agg,datassnfirstname14,datassnfirstname14agg)

###ssn last name
datassnlastname0agg <- datassnlastname0[, .(count = .N),by=record]
data$datassnlastname0agg = datassnlastname0agg$count

datassnlastname1agg <- datassnlastname1[, .(count = .N),by=record]
data$datassnlastname1agg = datassnlastname1agg$count

datassnlastname3agg <- datassnlastname3[, .(count = .N),by=record]
data$datassnlastname3agg = datassnlastname3agg$count

datassnlastname7agg <- datassnlastname7[, .(count = .N),by=record]
data$datassnlastname7agg = datassnlastname7agg$count

datassnlastname14agg <- datassnlastname14[, .(count = .N),by=record]
data$datassnlastname14agg = datassnlastname14agg$count

rm(datassnlastname0,datassnlastname0agg,datassnlastname1,datassnlastname1agg,datassnlastname3)
rm(datassnlastname3agg,datassnlastname7,datassnlastname7agg,datassnlastname14,datassnlastname14agg)

##ssn address
datassnaddress0agg <- datassnaddress0[, .(count = .N),by=record]
data$datassnaddress0agg = datassnaddress0agg$count

datassnaddress1agg <- datassnaddress1[, .(count = .N),by=record]
data$datassnaddress1agg = datassnaddress1agg$count

datassnaddress3agg <- datassnaddress3[, .(count = .N),by=record]
data$datassnaddress3agg = datassnaddress3agg$count

datassnaddress7agg <- datassnaddress7[, .(count = .N),by=record]
data$datassnaddress7agg = datassnaddress7agg$count

datassnaddress14agg <- datassnaddress14[, .(count = .N),by=record]
data$datassnaddress14agg = datassnaddress14agg$count

rm(datassnaddress0,datassnaddress0agg,datassnaddress1,datassnaddress1agg,datassnaddress3)
rm(datassnaddress3agg,datassnaddress7,datassnaddress7agg,datassnaddress14,datassnaddress14agg)

##ssn zip
datassnzip5_str0agg <- datassnzip5_str0[, .(count = .N),by=record]
data$datassnzip5_str0agg = datassnzip5_str0agg$count

datassnzip5_str1agg <- datassnzip5_str1[, .(count = .N),by=record]
data$datassnzip5_str1agg = datassnzip5_str1agg$count

datassnzip5_str3agg <- datassnzip5_str3[, .(count = .N),by=record]
data$datassnzip5_str3agg = datassnzip5_str3agg$count

datassnzip5_str7agg <- datassnzip5_str7[, .(count = .N),by=record]
data$datassnzip5_str7agg = datassnzip5_str7agg$count

datassnzip5_str14agg <- datassnzip5_str14[, .(count = .N),by=record]
data$datassnzip5_str14agg = datassnzip5_str14agg$count

rm(datassnzip5_str0,datassnzip5_str0agg,datassnzip5_str1,datassnzip5_str1agg,datassnzip5_str3)
rm(datassnzip5_str3agg,datassnzip5_str7,datassnzip5_str7agg,datassnzip5_str14agg,datassnzip5_str14)

##ssn dob
datassndob_str0agg <- datassndob_str0[, .(count = .N),by=record]
data$datassndob_str0agg = datassndob_str0agg$count

datassndob_str1agg <- datassndob_str1[, .(count = .N),by=record]
data$datassndob_str1agg = datassndob_str1agg$count

datassndob_str3agg <- datassndob_str3[, .(count = .N),by=record]
data$datassndob_str3agg = datassndob_str3agg$count

datassndob_str7agg <- datassndob_str7[, .(count = .N),by=record]
data$datassndob_str7agg = datassndob_str7agg$count

datassndob_str14agg <- datassndob_str14[, .(count = .N),by=record]
data$datassndob_str14agg = datassndob_str14agg$count

rm(datassndob_str0,datassndob_str0agg,datassndob_str1,datassndob_str1agg,datassndob_str3)
rm(datassndob_str3agg,datassndob_str7,datassndob_str7agg,datassndob_str14agg,datassndob_str14)

##ssn homephone
datassnhomephone0agg <- datassnhomephone0[, .(count = .N),by=record]
data$datassnhomephone0agg = datassnhomephone0agg$count

datassnhomephone1agg <- datassnhomephone1[, .(count = .N),by=record]
data$datassnhomephone1agg = datassnhomephone1agg$count

datassnhomephone3agg <- datassnhomephone3[, .(count = .N),by=record]
data$datassnhomephone3agg = datassnhomephone3agg$count

datassnhomephone7agg <- datassnhomephone7[, .(count = .N),by=record]
data$datassnhomephone7agg = datassnhomephone7agg$count

datassnhomephone14agg <- datassnhomephone14[, .(count = .N),by=record]
data$datassnhomephone14agg = datassnhomephone14agg$count

rm(datassnhomephone0,datassnhomephone0agg,datassnhomephone1,datassnhomephone1agg,datassnhomephone3)
rm(datassnhomephone3agg,datassnhomephone7,datassnhomephone7agg,datassnhomephone14agg,datassnhomephone14)

##ssn nameDOB
datassnnameDOB0agg <- datassnnameDOB0[, .(count = .N),by=record]
data$datassnnameDOB0agg = datassnnameDOB0agg$count

datassnnameDOB1agg <- datassnnameDOB1[, .(count = .N),by=record]
data$datassnnameDOB1agg = datassnnameDOB1agg$count

datassnnameDOB3agg <- datassnnameDOB3[, .(count = .N),by=record]
data$datassnnameDOB3agg = datassnnameDOB3agg$count

datassnnameDOB7agg <- datassnnameDOB7[, .(count = .N),by=record]
data$datassnnameDOB7agg = datassnnameDOB7agg$count

datassnnameDOB14agg <- datassnnameDOB14[, .(count = .N),by=record]
data$datassnnameDOB14agg = datassnnameDOB14agg$count

rm(datassnnameDOB0,datassnnameDOB0agg,datassnnameDOB1,datassnnameDOB1agg,datassnnameDOB3)
rm(datassnnameDOB3agg,datassnnameDOB7,datassnnameDOB7agg,datassnnameDOB14agg,datassnnameDOB14)

##ssn fulladdress
datassnfulladdress0agg <- datassnfulladdress0[, .(count = .N),by=record]
data$datassnfulladdress0agg = datassnfulladdress0agg$count

datassnfulladdress1agg <- datassnfulladdress1[, .(count = .N),by=record]
data$datassnfulladdress1agg = datassnfulladdress1agg$count

datassnfulladdress3agg <- datassnfulladdress3[, .(count = .N),by=record]
data$datassnfulladdress3agg = datassnfulladdress3agg$count

datassnfulladdress7agg <- datassnfulladdress7[, .(count = .N),by=record]
data$datassnfulladdress7agg = datassnfulladdress7agg$count

datassnfulladdress14agg <- datassnfulladdress14[, .(count = .N),by=record]
data$datassnfulladdress14agg = datassnfulladdress14agg$count

rm(datassnfulladdress0,datassnfulladdress0agg,datassnfulladdress1,datassnfulladdress1agg,datassnfulladdress3)
rm(datassnfulladdress3agg,datassnfulladdress7,datassnfulladdress7agg,datassnfulladdress14agg,datassnfulladdress14)

##firstname lastname
datafirstnamelastname0agg <- datafirstnamelastname0[, .(count = .N),by=record]
data$datafirstnamelastname0agg = datafirstnamelastname0agg$count

datafirstnamelastname1agg <- datafirstnamelastname1[, .(count = .N),by=record]
data$datafirstnamelastname1agg = datafirstnamelastname1agg$count

datafirstnamelastname3agg <- datafirstnamelastname3[, .(count = .N),by=record]
data$datafirstnamelastname3agg = datafirstnamelastname3agg$count

datafirstnamelastname7agg <- datafirstnamelastname7[, .(count = .N),by=record]
data$datafirstnamelastname7agg = datafirstnamelastname7agg$count

datafirstnamelastname14agg <- datafirstnamelastname14[, .(count = .N),by=record]
data$datafirstnamelastname14agg = datafirstnamelastname14agg$count

rm(datafirstnamelastname0,datafirstnamelastname0agg,datafirstnamelastname1,datafirstnamelastname1agg,datafirstnamelastname3)
rm(datafirstnamelastname3agg,datafirstnamelastname7,datafirstnamelastname7agg,datafirstnamelastname14agg,datafirstnamelastname14)

##firstname address
datafirstnameaddress0agg <- datafirstnameaddress0[, .(count = .N),by=record]
data$datafirstnameaddress0agg = datafirstnameaddress0agg$count

datafirstnameaddress1agg <- datafirstnameaddress1[, .(count = .N),by=record]
data$datafirstnameaddress1agg = datafirstnameaddress1agg$count

datafirstnameaddress3agg <- datafirstnameaddress3[, .(count = .N),by=record]
data$datafirstnameaddress3agg = datafirstnameaddress3agg$count

datafirstnameaddress7agg <- datafirstnameaddress7[, .(count = .N),by=record]
data$datafirstnameaddress7agg = datafirstnameaddress7agg$count

datafirstnameaddress14agg <- datafirstnameaddress14[, .(count = .N),by=record]
data$datafirstnameaddress14agg = datafirstnameaddress14agg$count

rm(datafirstnameaddress0,datafirstnameaddress0agg,datafirstnameaddress1,datafirstnameaddress1agg,datafirstnameaddress3)
rm(datafirstnameaddress3agg,datafirstnameaddress7,datafirstnameaddress7agg,datafirstnameaddress14agg,datafirstnameaddress14)

##first name zip5_str
datafirstnamezip5_str0agg <- datafirstnamezip5_str0[, .(count = .N),by=record]
data$datafirstnamezip5_str0agg = datafirstnamezip5_str0agg$count

datafirstnamezip5_str1agg <- datafirstnamezip5_str1[, .(count = .N),by=record]
data$datafirstnamezip5_str1agg = datafirstnamezip5_str1agg$count

datafirstnamezip5_str3agg <- datafirstnamezip5_str3[, .(count = .N),by=record]
data$datafirstnamezip5_str3agg = datafirstnamezip5_str3agg$count

datafirstnamezip5_str7agg <- datafirstnamezip5_str7[, .(count = .N),by=record]
data$datafirstnamezip5_str7agg = datafirstnamezip5_str7agg$count

datafirstnamezip5_str14agg <- datafirstnamezip5_str14[, .(count = .N),by=record]
data$datafirstnamezip5_str14agg = datafirstnamezip5_str14agg$count

rm(datafirstnamezip5_str0,datafirstnamezip5_str0agg,datafirstnamezip5_str1,datafirstnamezip5_str1agg,datafirstnamezip5_str3)
rm(datafirstnamezip5_str3agg,datafirstnamezip5_str7,datafirstnamezip5_str7agg,datafirstnamezip5_str14agg,datafirstnamezip5_str14)

##first name dob_str
datafirstnamedob_str0agg <- datafirstnamedob_str0[, .(count = .N),by=record]
data$datafirstnamedob_str0agg = datafirstnamedob_str0agg$count

datafirstnamedob_str1agg <- datafirstnamedob_str1[, .(count = .N),by=record]
data$datafirstnamedob_str1agg = datafirstnamedob_str1agg$count

datafirstnamedob_str3agg <- datafirstnamedob_str3[, .(count = .N),by=record]
data$datafirstnamedob_str3agg = datafirstnamedob_str3agg$count

datafirstnamedob_str7agg <- datafirstnamedob_str7[, .(count = .N),by=record]
data$datafirstnamedob_str7agg = datafirstnamedob_str7agg$count

datafirstnamedob_str14agg <- datafirstnamedob_str14[, .(count = .N),by=record]
data$datafirstnamedob_str14agg = datafirstnamedob_str14agg$count

rm(datafirstnamedob_str0,datafirstnamedob_str0agg,datafirstnamedob_str1,datafirstnamedob_str1agg,datafirstnamedob_str3)
rm(datafirstnamedob_str3agg,datafirstnamedob_str7,datafirstnamedob_str7agg,datafirstnamedob_str14agg,datafirstnamedob_str14)

##firstname homephone
datafirstnamehomephone0agg <- datafirstnamehomephone0[, .(count = .N),by=record]
data$datafirstnamehomephone0agg = datafirstnamehomephone0agg$count

datafirstnamehomephone1agg <- datafirstnamehomephone1[, .(count = .N),by=record]
data$datafirstnamehomephone1agg = datafirstnamehomephone1agg$count

datafirstnamehomephone3agg <- datafirstnamehomephone3[, .(count = .N),by=record]
data$datafirstnamehomephone3agg = datafirstnamehomephone3agg$count

datafirstnamehomephone7agg <- datafirstnamehomephone7[, .(count = .N),by=record]
data$datafirstnamehomephone7agg = datafirstnamehomephone7agg$count

datafirstnamehomephone14agg <- datafirstnamehomephone14[, .(count = .N),by=record]
data$datafirstnamehomephone14agg = datafirstnamehomephone14agg$count

rm(datafirstnamehomephone0,datafirstnamehomephone0agg,datafirstnamehomephone1,datafirstnamehomephone1agg,datafirstnamehomephone3)
rm(datafirstnamehomephone3agg,datafirstnamehomephone7,datafirstnamehomephone7agg,datafirstnamehomephone14agg,datafirstnamehomephone14)

##first name nameDOB
datafirstnamenameDOB0agg <- datafirstnamenameDOB0[, .(count = .N),by=record]
data$datafirstnamenameDOB0agg = datafirstnamenameDOB0agg$count

datafirstnamenameDOB1agg <- datafirstnamenameDOB1[, .(count = .N),by=record]
data$datafirstnamenameDOB1agg = datafirstnamenameDOB1agg$count

datafirstnamenameDOB3agg <- datafirstnamenameDOB3[, .(count = .N),by=record]
data$datafirstnamenameDOB3agg = datafirstnamenameDOB3agg$count

datafirstnamenameDOB7agg <- datafirstnamenameDOB7[, .(count = .N),by=record]
data$datafirstnamenameDOB7agg = datafirstnamenameDOB7agg$count

datafirstnamenameDOB14agg <- datafirstnamenameDOB14[, .(count = .N),by=record]
data$datafirstnamenameDOB14agg = datafirstnamenameDOB14agg$count

rm(datafirstnamenameDOB0,datafirstnamenameDOB0agg,datafirstnamenameDOB1,datafirstnamenameDOB1agg,datafirstnamenameDOB3)
rm(datafirstnamenameDOB3agg,datafirstnamenameDOB7,datafirstnamenameDOB7agg,datafirstnamenameDOB14agg,datafirstnamenameDOB14)

##firstname fulladdress
datafirstnamefulladdress0agg <- datafirstnamefulladdress0[, .(count = .N),by=record]
data$datafirstnamefulladdress0agg = datafirstnamefulladdress0agg$count

datafirstnamefulladdress1agg <- datafirstnamefulladdress1[, .(count = .N),by=record]
data$datafirstnamefulladdress1agg = datafirstnamefulladdress1agg$count

datafirstnamefulladdress3agg <- datafirstnamefulladdress3[, .(count = .N),by=record]
data$datafirstnamefulladdress3agg = datafirstnamefulladdress3agg$count

datafirstnamefulladdress7agg <- datafirstnamefulladdress7[, .(count = .N),by=record]
data$datafirstnamefulladdress7agg = datafirstnamefulladdress7agg$count

datafirstnamefulladdress14agg <- datafirstnamefulladdress14[, .(count = .N),by=record]
data$datafirstnamefulladdress14agg = datafirstnamefulladdress14agg$count

rm(datafirstnamefulladdress0,datafirstnamefulladdress0agg,datafirstnamefulladdress1,datafirstnamefulladdress1agg,datafirstnamefulladdress3)
rm(datafirstnamefulladdress3agg,datafirstnamefulladdress7,datafirstnamefulladdress7agg,datafirstnamefulladdress14agg,datafirstnamefulladdress14)

##lastname address
datalastnameaddress0agg <- datalastnameaddress0[, .(count = .N),by=record]
data$datalastnameaddress0agg = datalastnameaddress0agg$count

datalastnameaddress1agg <- datalastnameaddress1[, .(count = .N),by=record]
data$datalastnameaddress1agg = datalastnameaddress1agg$count

datalastnameaddress3agg <- datalastnameaddress3[, .(count = .N),by=record]
data$datalastnameaddress3agg = datalastnameaddress3agg$count

datalastnameaddress7agg <- datalastnameaddress7[, .(count = .N),by=record]
data$datalastnameaddress7agg = datalastnameaddress7agg$count

datalastnameaddress14agg <- datalastnameaddress14[, .(count = .N),by=record]
data$datalastnameaddress14agg = datalastnameaddress14agg$count

rm(datalastnameaddress0,datalastnameaddress0agg,datalastnameaddress1,datalastnameaddress1agg,datalastnameaddress3)
rm(datalastnameaddress3agg,datalastnameaddress7,datalastnameaddress7agg,datalastnameaddress14agg,datalastnameaddress14)

##lastname zip5_str
datalastnamezip5_str0agg <- datalastnamezip5_str0[, .(count = .N),by=record]
data$datalastnamezip5_str0agg = datalastnamezip5_str0agg$count

datalastnamezip5_str1agg <- datalastnamezip5_str1[, .(count = .N),by=record]
data$datalastnamezip5_str1agg = datalastnamezip5_str1agg$count

datalastnamezip5_str3agg <- datalastnamezip5_str3[, .(count = .N),by=record]
data$datalastnamezip5_str3agg = datalastnamezip5_str3agg$count

datalastnamezip5_str7agg <- datalastnamezip5_str7[, .(count = .N),by=record]
data$datalastnamezip5_str7agg = datalastnamezip5_str7agg$count

datalastnamezip5_str14agg <- datalastnamezip5_str14[, .(count = .N),by=record]
data$datalastnamezip5_str14agg = datalastnamezip5_str14agg$count

rm(datalastnamezip5_str0,datalastnamezip5_str0agg,datalastnamezip5_str1,datalastnamezip5_str1agg,datalastnamezip5_str3)
rm(datalastnamezip5_str3agg,datalastnamezip5_str7,datalastnamezip5_str7agg,datalastnamezip5_str14agg,datalastnamezip5_str14)

##lastname dob_str
datalastnamedob_str0agg <- datalastnamedob_str0[, .(count = .N),by=record]
data$datalastnamedob_str0agg = datalastnamedob_str0agg$count

datalastnamedob_str1agg <- datalastnamedob_str1[, .(count = .N),by=record]
data$datalastnamedob_str1agg = datalastnamedob_str1agg$count

datalastnamedob_str3agg <- datalastnamedob_str3[, .(count = .N),by=record]
data$datalastnamedob_str3agg = datalastnamedob_str3agg$count

datalastnamedob_str7agg <- datalastnamedob_str7[, .(count = .N),by=record]
data$datalastnamedob_str7agg = datalastnamedob_str7agg$count

datalastnamedob_str14agg <- datalastnamedob_str14[, .(count = .N),by=record]
data$datalastnamedob_str14agg = datalastnamedob_str14agg$count

rm(datalastnamedob_str0,datalastnamedob_str0agg,datalastnamedob_str1,datalastnamedob_str1agg,datalastnamedob_str3)
rm(datalastnamedob_str3agg,datalastnamedob_str7,datalastnamedob_str7agg,datalastnamedob_str14agg,datalastnamedob_str14)

##lastname homephone
datalastnamehomephone0agg <- datalastnamehomephone0[, .(count = .N),by=record]
data$datalastnamehomephone0agg = datalastnamehomephone0agg$count

datalastnamehomephone1agg <- datalastnamehomephone1[, .(count = .N),by=record]
data$datalastnamehomephone1agg = datalastnamehomephone1agg$count

datalastnamehomephone3agg <- datalastnamehomephone3[, .(count = .N),by=record]
data$datalastnamehomephone3agg = datalastnamehomephone3agg$count

datalastnamehomephone7agg <- datalastnamehomephone7[, .(count = .N),by=record]
data$datalastnamehomephone7agg = datalastnamehomephone7agg$count

datalastnamehomephone14agg <- datalastnamehomephone14[, .(count = .N),by=record]
data$datalastnamehomephone14agg = datalastnamehomephone14agg$count

rm(datalastnamehomephone0,datalastnamehomephone0agg,datalastnamehomephone1,datalastnamehomephone1agg,datalastnamehomephone3)
rm(datalastnamehomephone3agg,datalastnamehomephone7,datalastnamehomephone7agg,datalastnamehomephone14agg,datalastnamehomephone14)

##lastname nameDOB
datalastnamenameDOB0agg <- datalastnamenameDOB0[, .(count = .N),by=record]
data$datalastnamenameDOB0agg = datalastnamenameDOB0agg$count

datalastnamenameDOB1agg <- datalastnamenameDOB1[, .(count = .N),by=record]
data$datalastnamenameDOB1agg = datalastnamenameDOB1agg$count

datalastnamenameDOB3agg <- datalastnamenameDOB3[, .(count = .N),by=record]
data$datalastnamenameDOB3agg = datalastnamenameDOB3agg$count

datalastnamenameDOB7agg <- datalastnamenameDOB7[, .(count = .N),by=record]
data$datalastnamenameDOB7agg = datalastnamenameDOB7agg$count

datalastnamenameDOB14agg <- datalastnamenameDOB14[, .(count = .N),by=record]
data$datalastnamenameDOB14agg = datalastnamenameDOB14agg$count

rm(datalastnamenameDOB0,datalastnamenameDOB0agg,datalastnamenameDOB1,datalastnamenameDOB1agg,datalastnamenameDOB3)
rm(datalastnamenameDOB3agg,datalastnamenameDOB7,datalastnamenameDOB7agg,datalastnamenameDOB14agg,datalastnamenameDOB14)

##lastname fulladdress
datalastnamefulladdress0agg <- datalastnamefulladdress0[, .(count = .N),by=record]
data$datalastnamefulladdress0agg = datalastnamefulladdress0agg$count

datalastnamefulladdress1agg <- datalastnamefulladdress1[, .(count = .N),by=record]
data$datalastnamefulladdress1agg = datalastnamefulladdress1agg$count

datalastnamefulladdress3agg <- datalastnamefulladdress3[, .(count = .N),by=record]
data$datalastnamefulladdress3agg = datalastnamefulladdress3agg$count

datalastnamefulladdress7agg <- datalastnamefulladdress7[, .(count = .N),by=record]
data$datalastnamefulladdress7agg = datalastnamefulladdress7agg$count

datalastnamefulladdress14agg <- datalastnamefulladdress14[, .(count = .N),by=record]
data$datalastnamefulladdress14agg = datalastnamefulladdress14agg$count

rm(datalastnamefulladdress0,datalastnamefulladdress0agg,datalastnamefulladdress1,datalastnamefulladdress1agg,datalastnamefulladdress3)
rm(datalastnamefulladdress3agg,datalastnamefulladdress7,datalastnamefulladdress7agg,datalastnamefulladdress14agg,datalastnamefulladdress14)

##address zip5
dataaddresszip5_str0agg <- dataaddresszip5_str0[, .(count = .N),by=record]
data$dataaddresszip5_str0agg = dataaddresszip5_str0agg$count

dataaddresszip5_str1agg <- dataaddresszip5_str1[, .(count = .N),by=record]
data$dataaddresszip5_str1agg = dataaddresszip5_str1agg$count

dataaddresszip5_str3agg <- dataaddresszip5_str3[, .(count = .N),by=record]
data$dataaddresszip5_str3agg = dataaddresszip5_str3agg$count

dataaddresszip5_str7agg <- dataaddresszip5_str7[, .(count = .N),by=record]
data$dataaddresszip5_str7agg = dataaddresszip5_str7agg$count

dataaddresszip5_str14agg <- dataaddresszip5_str14[, .(count = .N),by=record]
data$dataaddresszip5_str14agg = dataaddresszip5_str14agg$count

rm(dataaddresszip5_str0,dataaddresszip5_str0agg,dataaddresszip5_str1,dataaddresszip5_str1agg)
rm(dataaddresszip5_str3,dataaddresszip5_str3agg,dataaddresszip5_str7,dataaddresszip5_str7agg,dataaddresszip5_str14agg,dataaddresszip5_str14)

##address dob
dataaddressdob_str0agg <- dataaddressdob_str0[, .(count = .N),by=record]
data$dataaddressdob_str0agg = dataaddressdob_str0agg$count
rm(dataaddressdob_str0agg,dataaddressdob_str0)

dataaddressdob_str1agg <- dataaddressdob_str1[, .(count = .N),by=record]
data$dataaddressdob_str1agg = dataaddressdob_str1agg$count
rm(dataaddressdob_str1,dataaddressdob_str1agg)

dataaddressdob_str3agg <- dataaddressdob_str3[, .(count = .N),by=record]
data$dataaddressdob_str3agg = dataaddressdob_str3agg$count
rm(dataaddressdob_str3,dataaddressdob_str3agg)

dataaddressdob_str7agg <- dataaddressdob_str7[, .(count = .N),by=record]
data$dataaddressdob_str7agg = dataaddressdob_str7agg$count
rm(dataaddressdob_str7,dataaddressdob_str7agg)

dataaddressdob_str14agg <- dataaddressdob_str14[, .(count = .N),by=record]
data$dataaddressdob_str14agg = dataaddressdob_str14agg$count
rm(dataaddressdob_str14,dataaddressdob_str14agg)


##address homephone
dataaddresshomephone0agg <- dataaddresshomephone0[, .(count = .N),by=record]
rm(dataaddresshomephone0)
data$dataaddresshomephone0agg = dataaddresshomephone0agg$count
rm(dataaddresshomephone0agg)
dataaddresshomephone1agg <- dataaddresshomephone1[, .(count = .N),by=record]
rm(dataaddresshomephone1)
data$dataaddresshomephone1agg = dataaddresshomephone1agg$count
rm(dataaddresshomephone1agg)
dataaddresshomephone3agg <- dataaddresshomephone3[, .(count = .N),by=record]
rm(dataaddresshomephone3)
data$dataaddresshomephone3agg = dataaddresshomephone3agg$count
rm(dataaddresshomephone3agg)
dataaddresshomephone7agg <- dataaddresshomephone7[, .(count = .N),by=record]
rm(dataaddresshomephone7)
data$dataaddresshomephone7agg = dataaddresshomephone7agg$count
rm(dataaddresshomephone7agg)
dataaddresshomephone14agg <- dataaddresshomephone14[, .(count = .N),by=record]
rm(dataaddresshomephone14)
data$dataaddresshomephone14agg = dataaddresshomephone14agg$count
rm(dataaddresshomephone14agg)

##address nameDOB I stoped here
###wirte data before address/nameDOB
#write.csv(data, file = "velocity_halfway.csv")

##address nameDOB
dataaddressnameDOB0agg <- dataaddressnameDOB0[, .(count = .N),by=record]
data$dataaddressnameDOB0agg = dataaddressnameDOB0agg$count

dataaddressnameDOB1agg <- dataaddressnameDOB1[, .(count = .N),by=record]
data$dataaddressnameDOB1agg = dataaddressnameDOB1agg$count

dataaddressnameDOB3agg <- dataaddressnameDOB3[, .(count = .N),by=record]
data$dataaddressnameDOB3agg = dataaddressnameDOB3agg$count

dataaddressnameDOB7agg <- dataaddressnameDOB7[, .(count = .N),by=record]
data$dataaddressnameDOB7agg = dataaddressnameDOB7agg$count

dataaddressnameDOB14agg <- dataaddressnameDOB14[, .(count = .N),by=record]
data$dataaddressnameDOB14agg = dataaddressnameDOB14agg$count

rm(dataaddressnameDOB0,dataaddressnameDOB0agg,dataaddressnameDOB1,dataaddressnameDOB1agg,dataaddressnameDOB3)
rm(dataaddressnameDOB3agg,dataaddressnameDOB7,dataaddressnameDOB7agg,dataaddressnameDOB14agg,dataaddressnameDOB14)

##address_fulladdress
dataaddressfulladdress0agg <- dataaddressfulladdress0[, .(count = .N),by=record]
data$dataaddressfulladdress0agg = dataaddressfulladdress0agg$count

dataaddressfulladdress1agg <- dataaddressfulladdress1[, .(count = .N),by=record]
data$dataaddressfulladdress1agg = dataaddressfulladdress1agg$count

dataaddressfulladdress3agg <- dataaddressfulladdress3[, .(count = .N),by=record]
data$dataaddressfulladdress3agg = dataaddressfulladdress3agg$count

dataaddressfulladdress7agg <- dataaddressfulladdress7[, .(count = .N),by=record]
data$dataaddressfulladdress7agg = dataaddressfulladdress7agg$count

dataaddressfulladdress14agg <- dataaddressfulladdress14[, .(count = .N),by=record]
data$dataaddressfulladdress14agg = dataaddressfulladdress14agg$count

rm(dataaddressfulladdress0,dataaddressfulladdress0agg,dataaddressfulladdress1,dataaddressfulladdress1agg,dataaddressfulladdress3)
rm(dataaddressfulladdress3agg,dataaddressfulladdress7,dataaddressfulladdress7agg,dataaddressfulladdress14agg,dataaddressfulladdress14)

##zip_5 dob
datazip5_strdob_str0agg <- datazip5_strdob_str0[, .(count = .N),by=record]
data$datazip5_strdob_str0agg = datazip5_strdob_str0agg$count

datazip5_strdob_str1agg <- datazip5_strdob_str1[, .(count = .N),by=record]
data$datazip5_strdob_str1agg = datazip5_strdob_str1agg$count

datazip5_strdob_str3agg <- datazip5_strdob_str3[, .(count = .N),by=record]
data$datazip5_strdob_str3agg = datazip5_strdob_str3agg$count

datazip5_strdob_str7agg <- datazip5_strdob_str7[, .(count = .N),by=record]
data$datazip5_strdob_str7agg = datazip5_strdob_str7agg$count

datazip5_strdob_str14agg <- datazip5_strdob_str14[, .(count = .N),by=record]
data$datazip5_strdob_str14agg = datazip5_strdob_str14agg$count

rm(datazip5_strdob_str0,datazip5_strdob_str0agg,datazip5_strdob_str1,datazip5_strdob_str1agg,datazip5_strdob_str3)
rm(datazip5_strdob_str3agg,datazip5_strdob_str7,datazip5_strdob_str7agg,datazip5_strdob_str14agg,datazip5_strdob_str14)

##zip5 homephone
datazip5_strhomephone0agg <- datazip5_strhomephone0[, .(count = .N),by=record]
data$datazip5_strhomephone0agg = datazip5_strhomephone0agg$count

datazip5_strhomephone1agg <- datazip5_strhomephone1[, .(count = .N),by=record]
data$datazip5_strhomephone1agg = datazip5_strhomephone1agg$count

datazip5_strhomephone3agg <- datazip5_strhomephone3[, .(count = .N),by=record]
data$datazip5_strhomephone3agg = datazip5_strhomephone3agg$count

datazip5_strhomephone7agg <- datazip5_strhomephone7[, .(count = .N),by=record]
data$datazip5_strhomephone7agg = datazip5_strhomephone7agg$count

datazip5_strhomephone14agg <- datazip5_strhomephone14[, .(count = .N),by=record]
data$datazip5_strhomephone14agg = datazip5_strhomephone14agg$count

rm(datazip5_strhomephone0,datazip5_strhomephone0agg,datazip5_strhomephone1,datazip5_strhomephone1agg,datazip5_strhomephone3)
rm(datazip5_strhomephone3agg,datazip5_strhomephone7,datazip5_strhomephone7agg,datazip5_strhomephone14agg,datazip5_strhomephone14)

##zip5 nameDOB
datazip5_strnameDOB0agg <- datazip5_strnameDOB0[, .(count = .N),by=record]
data$datazip5_strnameDOB0agg = datazip5_strnameDOB0agg$count

datazip5_strnameDOB1agg <- datazip5_strnameDOB1[, .(count = .N),by=record]
data$datazip5_strnameDOB1agg = datazip5_strnameDOB1agg$count

datazip5_strnameDOB3agg <- datazip5_strnameDOB3[, .(count = .N),by=record]
data$datazip5_strnameDOB3agg = datazip5_strnameDOB3agg$count

datazip5_strnameDOB7agg <- datazip5_strnameDOB7[, .(count = .N),by=record]
data$datazip5_strnameDOB7agg = datazip5_strnameDOB7agg$count

datazip5_strnameDOB14agg <- datazip5_strnameDOB14[, .(count = .N),by=record]
data$datazip5_strnameDOB14agg = datazip5_strnameDOB14agg$count

rm(datazip5_strnameDOB0,datazip5_strnameDOB0agg,datazip5_strnameDOB1,datazip5_strnameDOB1agg,datazip5_strnameDOB3)
rm(datazip5_strnameDOB3agg,datazip5_strnameDOB7,datazip5_strnameDOB7agg,datazip5_strnameDOB14agg,datazip5_strnameDOB14)

##zip5 fulladdress
datazip5_strfulladdress0agg <- datazip5_strfulladdress0[, .(count = .N),by=record]
data$datazip5_strfulladdress0agg = datazip5_strfulladdress0agg$count

datazip5_strfulladdress1agg <- datazip5_strfulladdress1[, .(count = .N),by=record]
data$datazip5_strfulladdress1agg = datazip5_strfulladdress1agg$count

datazip5_strfulladdress3agg <- datazip5_strfulladdress3[, .(count = .N),by=record]
data$datazip5_strfulladdress3agg = datazip5_strfulladdress3agg$count

datazip5_strfulladdress7agg <- datazip5_strfulladdress7[, .(count = .N),by=record]
data$datazip5_strfulladdress7agg = datazip5_strfulladdress7agg$count

datazip5_strfulladdress14agg <- datazip5_strfulladdress14[, .(count = .N),by=record]
data$datazip5_strfulladdress14agg = datazip5_strfulladdress14agg$count

rm(datazip5_strfulladdress0,datazip5_strfulladdress0agg,datazip5_strfulladdress1,datazip5_strfulladdress1agg,datazip5_strfulladdress3)
rm(datazip5_strfulladdress3agg,datazip5_strfulladdress7,datazip5_strfulladdress7agg,datazip5_strfulladdress14agg,datazip5_strfulladdress14)

##dob homephone can not have 7 and 14
datadob_strhomephone0agg <- datadob_strhomephone0[, .(count = .N),by=record]
data$datadob_strhomephone0agg = datadob_strhomephone0agg$count
rm(datadob_strhomephone0,data)

datadob_strhomephone1agg <- datadob_strhomephone1[, .(count = .N),by=record]
data$datadob_strhomephone1agg = datadob_strhomephone1agg$count

datadob_strhomephone3agg <- datadob_strhomephone3[, .(count = .N),by=record]
data$datadob_strhomephone3agg = datadob_strhomephone3agg$count

##datadob_strhomephone7agg <- datadob_strhomephone7[, .(count = .N),by=record]
##data$datadob_strhomephone7agg = datadob_strhomephone7agg$count

##datadob_strhomephone14agg <- datadob_strhomephone14[, .(count = .N),by=record]
##data$datadob_strhomephone14agg = datadob_strhomephone14agg$count

rm(datadob_strhomephone0,datadob_strhomephone0agg,datadob_strhomephone1,datadob_strhomephone1agg,datadob_strhomephone3)
rm(datadob_strhomephone3agg)
##datadob_strhomephone7,datadob_strhomephone7agg,datadob_strhomephone14agg,datadob_strhomephone14


##dob nameDOB
datadob_strnameDOB0agg <- datadob_strnameDOB0[, .(count = .N),by=record]
data$datadob_strnameDOB0agg = datadob_strnameDOB0agg$count

datadob_strnameDOB1agg <- datadob_strnameDOB1[, .(count = .N),by=record]
data$datadob_strnameDOB1agg = datadob_strnameDOB1agg$count

datadob_strnameDOB3agg <- datadob_strnameDOB3[, .(count = .N),by=record]
data$datadob_strnameDOB3agg = datadob_strnameDOB3agg$count

datadob_strnameDOB7agg <- datadob_strnameDOB7[, .(count = .N),by=record]
data$datadob_strnameDOB7agg = datadob_strnameDOB7agg$count

datadob_strnameDOB14agg <- datadob_strnameDOB14[, .(count = .N),by=record]
data$datadob_strnameDOB14agg = datadob_strnameDOB14agg$count

rm(datadob_strnameDOB0,datadob_strnameDOB0agg,datadob_strnameDOB1,datadob_strnameDOB1agg,datadob_strnameDOB3)
rm(datadob_strnameDOB3agg,datadob_strnameDOB7,datadob_strnameDOB7agg,datadob_strnameDOB14agg,datadob_strnameDOB14)

##dob fulladdress
datadob_strfulladdress0agg <- datadob_strfulladdress0[, .(count = .N),by=record]
data$datadob_strfulladdress0agg = datadob_strfulladdress0agg$count

datadob_strfulladdress1agg <- datadob_strfulladdress1[, .(count = .N),by=record]
data$datadob_strfulladdress1agg = datadob_strfulladdress1agg$count

datadob_strfulladdress3agg <- datadob_strfulladdress3[, .(count = .N),by=record]
data$datadob_strfulladdress3agg = datadob_strfulladdress3agg$count

datadob_strfulladdress7agg <- datadob_strfulladdress7[, .(count = .N),by=record]
data$datadob_strfulladdress7agg = datadob_strfulladdress7agg$count

datadob_strfulladdress14agg <- datadob_strfulladdress14[, .(count = .N),by=record]
data$datadob_strfulladdress14agg = datadob_strfulladdress14agg$count

rm(datadob_strfulladdress0,datadob_strfulladdress0agg,datadob_strfulladdress1,datadob_strfulladdress1agg,datadob_strfulladdress3)
rm(datadob_strfulladdress3agg,datadob_strfulladdress7,datadob_strfulladdress7agg,datadob_strfulladdress14agg,datadob_strfulladdress14)

##homephone nameDOB
datahomephonenameDOB0agg <- datahomephonenameDOB0[, .(count = .N),by=record]
data$datahomephonenameDOB0agg = datahomephonenameDOB0agg$count

datahomephonenameDOB1agg <- datahomephonenameDOB1[, .(count = .N),by=record]
data$datahomephonenameDOB1agg = datahomephonenameDOB1agg$count

datahomephonenameDOB3agg <- datahomephonenameDOB3[, .(count = .N),by=record]
data$datahomephonenameDOB3agg = datahomephonenameDOB3agg$count

datahomephonenameDOB7agg <- datahomephonenameDOB7[, .(count = .N),by=record]
data$datahomephonenameDOB7agg = datahomephonenameDOB7agg$count

datahomephonenameDOB14agg <- datahomephonenameDOB14[, .(count = .N),by=record]
data$datahomephonenameDOB14agg = datahomephonenameDOB14agg$count

rm(datahomephonenameDOB0,datahomephonenameDOB0agg,datahomephonenameDOB1,datahomephonenameDOB1agg,datahomephonenameDOB3)
rm(datahomephonenameDOB3agg,datahomephonenameDOB7,datahomephonenameDOB7agg,datahomephonenameDOB14agg,datahomephonenameDOB14)

##homephone fulladdress
datahomephonefulladdress0agg <- datahomephonefulladdress0[, .(count = .N),by=record]
data$datahomephonefulladdress0agg = datahomephonefulladdress0agg$count

datahomephonefulladdress1agg <- datahomephonefulladdress1[, .(count = .N),by=record]
data$datahomephonefulladdress1agg = datahomephonefulladdress1agg$count

datahomephonefulladdress3agg <- datahomephonefulladdress3[, .(count = .N),by=record]
data$datahomephonefulladdress3agg = datahomephonefulladdress3agg$count

datahomephonefulladdress7agg <- datahomephonefulladdress7[, .(count = .N),by=record]
data$datahomephonefulladdress7agg = datahomephonefulladdress7agg$count

datahomephonefulladdress14agg <- datahomephonefulladdress14[, .(count = .N),by=record]
data$datahomephonefulladdress14agg = datahomephonefulladdress14agg$count

rm(datahomephonefulladdress0,datahomephonefulladdress0agg,datahomephonefulladdress1,datahomephonefulladdress1agg,datahomephonefulladdress3)
rm(datahomephonefulladdress3agg,datahomephonefulladdress7,datahomephonefulladdress7agg,datahomephonefulladdress14agg,datahomephonefulladdress14)

##nameDOB fulladdress
datanameDOBfulladdress0agg <- datanameDOBfulladdress0[, .(count = .N),by=record]
data$datanameDOBfulladdress0agg = datanameDOBfulladdress0agg$count

datanameDOBfulladdress1agg <- datanameDOBfulladdress1[, .(count = .N),by=record]
data$datanameDOBfulladdress1agg = datanameDOBfulladdress1agg$count

datanameDOBfulladdress3agg <- datanameDOBfulladdress3[, .(count = .N),by=record]
data$datanameDOBfulladdress3agg = datanameDOBfulladdress3agg$count

datanameDOBfulladdress7agg <- datanameDOBfulladdress7[, .(count = .N),by=record]
data$datanameDOBfulladdress7agg = datanameDOBfulladdress7agg$count

datanameDOBfulladdress14agg <- datanameDOBfulladdress14[, .(count = .N),by=record]
data$datanameDOBfulladdress14agg = datanameDOBfulladdress14agg$count

rm(datanameDOBfulladdress0,datanameDOBfulladdress0agg,datanameDOBfulladdress1,datanameDOBfulladdress1agg,datanameDOBfulladdress3)
rm(datanameDOBfulladdress3agg,datanameDOBfulladdress7,datanameDOBfulladdress7agg,datanameDOBfulladdress14agg,datanameDOBfulladdress14)

###make up fpr previours missing homephones
#assign(paste0("data",'homephone',3), time_window(data2, 3, 'homephone'))
##still can not run due to memory problems

rm(data3)

for (i in c(7,14)){ 
  assign(paste0("data",c('dob_str','homephone')[1],c('dob_str','homephone')[2],i), time_window(data2, i,c('dob_str','homephone') ))
}
datadob_strhomephone7agg <- datadob_strhomephone7[, .(count = .N),by=record]
rm(datadob_strhomephone7)
data$datadob_strhomephone7agg = datadob_strhomephone7agg$count
rm(datadob_strhomephone7agg)

#assign(paste0("data",c('dob_str','homephone')[1],c('dob_str','homephone')[2],14), time_window(data2, 14,c('dob_str','homephone') ))


#################################################################################
##data has 210 variables
##should have 40*5+14 = 214 variables
#missing 4:
##datahomephone3
##datahomephone7
##datahomephone14
##datadob_strhomephone14
#################################################################################
write.csv(data, file = "applications_velocity.csv")



