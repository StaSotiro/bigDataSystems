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

# Maybe add users in Sorted Sets? <- Don't run for now... I see no point atm

count=0

for(listing in listings){
  
  if(listing$MonthID ==1){
    print("Adding to Jan")
    redis$ZADD("modJan", "0")
  }
  if(listing$MonthID==2){
    print("Adding to FEB")
    redis$ZADD("modFeb", count, 1)
  }
  if(listing$MonthID==3){
    print("Adding to MAR")
    redis$ZADD("modMar", count, 1)
  }
}


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


# Task 5

# Generate two bitmaps, emails opened and udpated and compare the two
curUser = emails$UserID[1]
count=0

for(i in 0:(nrow(emails)-1)){
  if(emails$UserID[i+1] != curUser){
    curUser = emails$UserID[i+1]
    count= count + 1
  } 
  if(emails$MonthID[i+1]==1 && emails$EmailOpened[i+1]==0){
    redis$SETBIT("mailNotOpenedJan", count, 1)
  }
}

# bitop and notOpenAndUpdated ModificationsJanuary mailNotOpenedJan
print(paste("Customers that received an email, did not open it, yet modified listings in Jan: ", redis$BITCOUNT("notOpenAndUpdated"), " out of ", length(januaryListings)))

# Task 6 Just like the above, but you need to create it for the other two months, then do a BITMAP OR of the final 3 Bitmaps 
# it's for U <3 


# Task 7 Summary of results
