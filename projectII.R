#Local Connection
redis = redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))


setwd("/Users/stavros/Documents/BI/BigDataSystems/Redis-Mongo Assignment-v1.3/RECORDED_ACTIONS")

emails = read.csv("emails_dummy.csv", sep = ",")
listings = read.csv("modified_dummy.csv", sep = ",")

str(emails)
str(listings)

# Maybe add users in Hashes? 

# Task 1 

# Fetch all items of month ==1 from the list, add them to Redis as bitmap
januaryListings = listings[listings$MonthID==1,"ModifiedListing"]


for(i in 0:length(januaryListings)-1){
  if(januaryListings[i+1]==1){
    redis$SETBIT("ModificationsJanuary", i, 1)
  }
}

print(paste("Customers that modified listings in Jan: ", redis$BITCOUNT("ModificationsJanuary"), " out of ", length(januaryListings)))

# Task 2
redis$BITOP("NOT", "NotModsJan", "ModificationsJanuary")

print(paste("Customers that modified listings in Jan: ", redis$BITCOUNT("NotModsJan"), " out of ", length(januaryListings)))

# Comparison doesn't show all the modifications, because the check is on byte level, whereas we store the data in bit level
# That means that the last Listings % 8 will not be compared, so their values will not be moved 

# Validate the statement by adding 1,2,3,..8 and see the results

curUser = emails$UserID[1]
count=0

for(i in 0:(nrow(emails)-1)){
  print(i)
  if(emails$UserID[i+1] != curUser){
    print(paste( "Changing User", i))
    curUser = emails$UserID[i+1]
    count= count + 1
  } 
  if(emails$MonthID[i+1]==1){
    print("Adding to Jan")
    redis$SETBIT("mailJan", count, 1)
  }
  if(emails$MonthID[i+1]==2){
    print("Adding to FEB")
    redis$SETBIT("mailFeb", count, 1)
  }
  if(emails$MonthID[i+1]==3){
    print("Adding to MAR")
    redis$SETBIT("mailMar", count, 1)
  }
}

# run on CLI -> BITOP AND allMailsThreeMonths mailJan mailFeb mailMar
# redis$BITOP("AND", "allMailsThreeMonths", "mailJan", "mailFeb", "mailMar")

totalSum = redis$BITCOUNT("allMailsThreeMonths")
# Here we need to add some logic to calculate the last emailsSent % 8 and do it by hand
print(paste("Customers that got email in all 3 months: ", totalSum))


# Task 4 

redis$BITOP("NOT","NotMailFeb", "mailFeb")

# BITOP AND mailsJanMar mailJan NotMailFeb mailMar

totalSum = redis$BITCOUNT("mailsJanMar")
