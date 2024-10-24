print("Script is starting...")


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc)

### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # Replace Sys.getenv("UID") with your email
                                     authenticator = "externalbrowser")
  print("Database Connected!")  # Success message
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error handling
})

# Set the schema for the session
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")

absentee_q <- "SELECT 
                 'HA' AS AIRLINE,
                 absence_date, 
                 base, 
                 crew_type, 
                 fleet, 
                 seat, 
                 'SICK_LEAVE' AS TYPE,
                 COUNT(DISTINCT MASTID_EMPNO) AS COUNT  
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('1SC','1SK','2SK','3SK','5SC','5SK','SIC','SK3','SOP')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                 'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'FATIGUE' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('FAT', 'FAP')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                   'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'UNION_BUSINESS' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('UNI', 'UN2')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                  'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'MILITARY' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('MIL')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                 'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'EMERGENCY_LEAVE' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('1EF','2EF','5EF','9EF')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                 'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'FMLA' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('FLP','FLS','FLU','FLV')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                 'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'JURY_DUTY' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('1JD','2JD','5JD','9JD')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                 'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'NO_SHOW' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('N/S')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               UNION ALL
                
               SELECT 
                 'HA' AS AIRLINE,
                   absence_date, 
                   base, 
                   crew_type, 
                   fleet, 
                   seat, 
                   'BEREAVEMENT' AS TYPE,
                   COUNT(DISTINCT MASTID_EMPNO) AS COUNT   
               FROM PLAYGROUND.CREW_ANALYTICS.AA_CONSOLIDATED_ABSENTEE
               WHERE CODE_LATEST_UPDATE IN ('1DF','2DF','5DF','9DF')
               GROUP BY absence_date, base, crew_type, fleet, seat
                
               ORDER BY ABSENCE_DATE ASC;
               "

clean_absentee <- dbGetQuery(db_connection_pg, absentee_q)

write_csv(clean_absentee, "F:/INFLIGHT_ABSENTEE.csv")

print("Data Uploaded")
Sys.sleep(10)
