library("mongolite")
library("jsonlite")
library("stringr")
library("sjmisc")
setwd("/Users/stavros/Documents/BI/BigDataSystems/Redis-Mongo Assignment-v1.3/BIKES")

user = ""
pass = ""
host = ""

uri = sprintf("mongodb+srv://%s:%s@%s/assignment?retryWrites=true&w=majority", user, pass, host)
bikes <- mongo(collection = "bikes",  db = "assignment", url = uri)
categories <- mongo(collection = "categories",  db = "assignment", url = uri)
brands <- mongo(collection = "brands",  db = "assignment", url = uri)

# We'd split sellers, but there are no interest in sellers in this business case
# sellers <- mongo(collection = "sellers",  db = "assignment", url = uri)

fileList = readLines("files_list.txt")

# Inserting the data

checkCategory = function (cat){
  foundCat = categories$find(paste('{"name":"',cat,'"}',sep = ""), fields = '{"_id": true}')
  if( length(foundCat)== 0){
    categories$insert(paste('{"name":"',cat,'"}',sep = ""))
    foundCat = categories$find(paste('{"name":"',cat,'"}',sep = ""), fields = '{"_id": true}')
  }
  return(foundCat$`_id`)
}

checkBrand = function (brand){
  foundBrand = brands$find(paste('{"name":"',brand,'"}',sep = ""), fields = '{"_id": true}')
  if( length(foundBrand)== 0){
    brands$insert(paste('{"name":"',brand,'"}',sep = ""))
    foundBrand = brands$find(paste('{"name":"',brand,'"}',sep = ""), fields = '{"_id": true}')
  }
  return(foundBrand$`_id`)
}

for(filePath in fileList){
  json_data = fromJSON(readLines(filePath, encoding="UTF-8"))
  price = gsub("[^0-9]", '' ,json_data$ad_data$Price)
  if(is_empty(price)){
    price = "0"
  }
  json_data$ad_data$Price = as.numeric(price)
  
  power = gsub("[^0-9]", '' ,json_data$ad_data$Power)
  if(is_empty(power)){
    power = "0"
  }
  json_data$ad_data$Power = as.numeric(power)
  
  miles = gsub("[^0-9]", '' ,json_data$ad_data$Mileage)
  if(is_empty(miles)){
    miles = "0"
  }
  json_data$ad_data$Mileage = as.numeric(miles)
  
  cc = gsub("[^0-9]", '' ,json_data$ad_data$`Cubic capacity`)
  if(is_empty(cc)){
    cc = "0"
  }
  json_data$ad_data$`Cubic capacity` = as.numeric(cc)
  
  # Add primary category
  cat = gsub("[ ]", '', str_split(json_data$ad_data$Category, "-",simplify = T)[2])
  json_data$ad_data$Category = checkCategory(cat)
  
  # Add brand
  brand = json_data$metadata$brand
  json_data$metadata$brand = checkBrand(brand)
  
  # Add registration month & year
  json_data$ad_data$Month = as.numeric(gsub("[ ]", '', str_split(json_data$ad_data$Registration, "/",simplify = T)[1]))
  
  mon = gsub("[ ]", '', str_split(json_data$ad_data$Registration, "/",simplify = T)[1])
  if(is_empty(mon)){
    mon = "1"
  }
  json_data$ad_data$Month = as.numeric(mon)
  
  
  yr = gsub("[ ]", '', str_split(json_data$ad_data$Registration, "/",simplify = T)[2])
  if(is_empty(yr)){
    yr = floor(bikes$aggregate('[{"$group":{"_id":"_id", "avgprice": {"$avg":"$ad_data.Year"}, "count": {"$sum":1}}}]')$avgprice)
  }else{
    yr = as.numeric(yr)
  }
  json_data$ad_data$Year = as.numeric(gsub("[ ]", '', str_split(json_data$ad_data$Registration, "/",simplify = T)[2]))
  
  # if(!is.numeric(json_data$ad_data$Year)){
  #   avgYear = floor(bikes$aggregate('[{"$group":{"_id":"_id", "avgprice": {"$avg":"$ad_data.Year"}, "count": {"$sum":1}}}]')$avgprice)
  #   json_data$ad_data$Year=avgYear
  # }
  
  bikes$insert(toJSON(json_data, auto_unbox = TRUE))
}

# 2 -> 
# Counting all the input rows 29701
bikes$count('{}') 

# 3 

bikes$aggregate('[{"$match":{"ad_data.Price":{"$gt":50}}}, {"$group":{"_id":"_id","avgprice": {"$avg":"$ad_data.Price"}, "count": {"$sum":1}}}]')

# Here we check the ads that do not have 0 value in Price, which we set when there is non provided. AVGPrice = 3029€ with 28505
# Usually you have sellers with ad value of 1€ or 10€ so they get first when sorting. We decided to make that obsolete to get a better understanding


# 4

# bikes$aggregate('[{"$group": {"_id":null, "maxPrice":{"$max":"$ad_data.Price"}}}]') -> Not performant
bikes$find("{}", sort = '{"ad_data.Price":-1}', limit = 1, fields = '{"title":true, "ad_data.Price":true}')
# Bmw  HP4 '17 - 89000€



# bikes$aggregate('[{"$match":{"ad_data.Price":{"$gt":100}}}, {"$group": {"_id":null, "maxPrice":{"$min":"$ad_data.Price"}}}]')

# Find a proper price 

bikes$find(query = '{"ad_data.Price":{"$gt":50, "$lt":300}}',fields = '{"title":true, "ad_data.Price":true, "description":true}', limit = 10)

# Checking a few ads on the 50-300 range to get some insight of the data, most ads refer to parts, or collectors bikes, but do not 
# seem like a scam or something similar, so we decide on using 100 as our lowest tier. We would choose a higher value if we were looking
# for a fully functional bike

bikes$find(query = '{"ad_data.Price":{"$gt":100}}', sort = '{"ad_data.Price":1}', limit = 1, fields = '{"title":true, "ad_data.Price":true}')
# Honda CBR 600RR  '05- 101€
# -> We run the query starting from 150, since all the 0s are not worth looking at, and up to 100 the ads typically had no information
# so we considered them "not proper". Also the descriptions in some of the queries were not informative, leading to believe that it's just 
# random stuff


# 5
bikes$index(remove = "title_text_description_text")
bikes$index('{"title":"text","description":"text","metadata.model":"text"}')
negotatable = bikes$find( '{"$text":{"$search":"Negotiable"} }')
length(negotatable)

# bikes$aggregate('[{"$text":{"$search":"Negotiable"} }, {"$count":"Total"}]') 
# Atlas tier (free ofc) doesn't allow aggregation with text

# Total: At least 8. Should be more, since we didn't search in Greek and some descriptions include it in Greek, maybe the ads with 0 price tag
# Are the negotiable price tags as well

# 6 
# it will also require a group by brandId and add a count like before, but can't be done in the aggregation due to tier
# A possible query could be something like:

# bikes$aggregate('[{"$text":{"$search":"Negotiable"} }, {"$count":"Total"}]') 

# 7

bikes$aggregate('[{"$match":{"ad_data.Price":{"$gt":300}}}, {"$group":{"_id":"$metadata.brand", "avgprice": {"$avg":"$ad_data.Price"}}}, {"$sort":{"avgprice":-1}}, {"$limit":10}]')

# Semog 15600.000
# Indian 13716.667
# Harley Davidson 12636.425
# Victory 12500.000
# Rewaco 12000.000
# Polaris 11120.594
# Mv Agusta 10751.000
# CAN-AM 10550.000
# Arctic Cat  8971.286
# Lambretta  8245.455

# 8

bikes$aggregate('[{"$match":{"ad_data.Price":{"$gt":300}}}, {"$group":{"_id":"$ad_data.Make/Model", "avgage": {"$avg":"$ad_data.Year"}}}, {"$sort":{"avgage":1}}, {"$limit":10}]')

#                 Daytona DY 50 '01   1901
#               Daelim S-FIVE '01   1901
#                Yamaha TDM 850 '08   1908
# Αλλο henderson indian replica '31   1931
#                  Bmw R 12   '34   1934
#         Αλλο MATCHLESS G3 350 '35   1935
#                    Norton H16 '36   1936
#                Αλλο Matsoules '38   1938
#           Αλλο Matchless G3/L '39   1939
#           Bsa M20 ARMY MOTO! '39   1939

# As we can see there should have been some cleaning done in the model name that we overlooked to clean the αλλο word.
# We don't think that removing the year from the model name is correct, since usually classy versions are known by their release year

