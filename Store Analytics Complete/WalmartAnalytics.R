# Load Data into a DataFrame
library(readr)
library(tidyverse)
## library(plyr) #KILLS dplyr
library(dplyr)
library(lubridate)

# DateTime is stored as 13 digit integer
# This enables loading via Double
options(digits=13)
options("digits")

# 213,495 ROWS 10 VARIABLES
# INITIAL FILE
data_org <- read_tsv("~/Apps/Walmart/4729-2038.tsv",col_names=TRUE,trim_ws = TRUE,col_types = 
                   list(col_character(),col_character(),col_double(),col_integer(),col_integer(),
                   col_integer(),col_double(),col_character(),col_character(),col_character()))
summary(data_org[-(1:2)])  ## File spans Fri, July 21, 2017 8:48 PM to Sun, July 23, 2017 7:59 PM GMT - 4 DST

# CHECK for DUPLICATED ROWS  # NO DUPLICATES
anyDuplicated(data_org)

# FILE AFTER CLEANING Before Extracting Finance & Gas
# 213,475 ROWS
data <- read_tsv("~/Apps/Walmart/data_clean.tsv",col_names=TRUE,trim_ws = TRUE,col_types = 
                   list(col_character(),col_character(),col_double(),col_integer(),col_integer(),
                   col_integer(),col_double(),col_character(),col_character(),col_character()))

# VIEW DATA for Filtering and Anomalies
View(data)  ## A data window

# ANALYSIS FOR ANOMOLIES & ELIMINATION
# Select amounts greater than 200 to examine
greater_200 <- subset(data, amount >= 200)
# LIMIT Columns Displayed
greater_200_01 <- greater_200[c(-1,-3,-4,-6,-10)]
View(greater_200_01)  ## Departments 86 & 99 are Financial Transactions

# Control Number of lines to pring
options(dplyr.print_max = 500)
options(max.print=300)
tibble.print_max = 500

# Discovered the following Financial Transactions
# Filter in finance file
finance <- filter(data, (upc == '0000000000040638437479' | upc == '0000000000605388122360' |
                           upc == '0000000000605388122330' | upc == '0000000000605388190350' |
                           upc == '0000000000605388963090' | upc == '0000000000605388122390' |
                           upc == '0000000000079936640209' | upc == '0000000000060538802945' |
                           department == 86
                        ) )
View(finance)  ## 582 entries
# upc_select <- subset(data, upc='0000000000605388122330')

# ELIMINATE FINANCE for separate analysis
# data_01 212,893 ROWS
data_01 <- filter(data, (upc != '0000000000040638437479' & upc != '0000000000605388122360' &
                           upc != '0000000000605388122330' & upc != '0000000000605388190350' &
                           upc != '0000000000605388963090' & upc != '0000000000605388122390' &
                           upc != '0000000000079936640209' & upc != '0000000000060538802945' &
                           department != 86
) )

# Examine DISTRIBUTION OF DATA
summary(data_01[-(1:2)])  ## File spans Fri, July 21, 2017 8:00 PM to Sun, July 23, 2017 7:59 PM GMT - 4 DST
View(data_01)

# EXAMINE FLAG & ELIMINATE OUTLYERS
# -40004.00 & 40004.00 Scan correction 2222 Dept 33 Reg 15 7/22 10:32 PM +/- 40004.00

# Filter out Gas
# 2,868 ROWS
gasoline <- filter(data_01, upc == '0000000000658441000000' | upc == '0000000000658455000000')

# ELIMINATE GASOLINE for separate analysis 0000000000658455000000
# 210,005 ROWS
data_02 <- filter(data_01, upc != '0000000000658441000000' & upc != '0000000000658455000000')

# EXAMINE SCAN ERRORS -- LARGE NEGATIVES  # investigate UPC: 0000000006937419204870
returns_over_100 <- filter(data_02, amount <= -100.0)

# Examine suspicious returns
View(filter(data_02, upc == '0000000006937419204870'))

# SEGMENT by Stores
# 2222 has 176,241 ROWS
# 3333 has 33,774 ROWS
s_2222 <- filter(data_02, store_number == 2222)
s_3333 <- filter(data_02, store_number == 3333)

ms_to_date = function(ms, t0="1970-01-01", timezone) {
  ## @ms: a numeric vector of milliseconds (big integers of 13 digits)
  ## @t0: a string of the format "yyyy-mm-dd", specifying the date that
  ##      corresponds to 0 millisecond
  ## @timezone: a string specifying a timezone that can be recognized by R
  ## return: a POSIXct vector representing calendar dates and times        
  sec = ms / 1000
  as.POSIXct(sec, origin=t0, tz=timezone)
}

# CONVERT POSIX time and add day & time
store_2222 <- mutate(s_2222,day=day(ms_to_date(order_time,timezone="")),hour=hour(ms_to_date(order_time,timezone="")))
store_3333 <- mutate(s_3333,day=day(ms_to_date(order_time,timezone="")),hour=hour(ms_to_date(order_time,timezone="")))

# Do not display columns 1,4,6,8,10
View(select(store_3333,order_id,department,amount,day,hour))

# MISSING DATA?  Examine by time
# Store 3333 has MISSING DATA
store_2222 %>% group_by(day,hour) %>% summarise(SpendbyHour=sum(amount)) %>% arrange(day,hour)
store_3333 %>% group_by(day,hour) %>% summarise(SpendbyHour=sum(amount)) %>% arrange(day,hour)

# Store to Store Comparison Spending by Day
store_2222 %>% group_by(day) %>% summarise(SpendbyHour=sum(amount)) %>% arrange(day)
store_3333 %>% group_by(day) %>% summarise(SpendbyHour=sum(amount)) %>% arrange(day)

dplyr::summarise(by_order,sum(amount) )
store_3333 %>% group_by(order_id) %>% summarise(items=n(), basket=sum(amount)/items) %>% arrange(desc(basket))

# ITEMS > $100
store_2222 %>% filter(amount >= 100.0) %>% select(order_id,department,amount,name) %>% arrange(desc(amount))
store_3333 %>% filter(amount >= 100.0) %>% select(order_id,department,amount,name) %>% arrange(desc(amount))

# Department Summary
# Total Spending
store_2222 %>% summarise(items=n(), DeptTotal=sum(amount)) %>% arrange(desc(DeptTotal))
# Spending by Department
store_2222 %>% group_by(department) %>% summarise(items=n(), DeptTotal=sum(amount)) %>% arrange(desc(DeptTotal))

# Spending by Customer ID
tibble.print_max = Inf
View(store_2222 %>% group_by(order_id) %>% summarise(items=n(), basket=sum(amount)/items) %>% arrange(desc(basket)))
View(store_2222)

head(data)
class(data)   ### We have a DataFrame
typeof(data)  ### A list
dim(data)     ### 21345 x 10 variables
summary(data[-(1:2)])  ## File spans Sat, July 22, 2017 12:00 AM to Sun, July 23, 2017 11:59 PM GMT - 4 DST
summary(data[c(3,7)])



date_in_ms = c(1500720834000,1500719696000)
# ms_to_date(date_in_ms,timezone="America/New_York")
dateTime <- ms_to_date(date_in_ms,timezone="")
# Analysis
sum_dept <- ddply(data_02,c("store_number","department"),summarise,
                  N    = length(amount),
                  mean = mean(amount),
                  sd   = sd(amount),
                  se   = sd/sqrt(N)
)
sum_dept
sum_cust <- ddply(data_02,c("order_id"),summarise,
                  N    = length(amount),
                  mean = mean(amount),
                  sd   = sd(amount),
                  se   = sd/sqrt(N)
)

detach(package:plyr)
