# Data Dictionary

We will have 1 fact table and 2 dimension tables in star schema:
1 fact table with information from I94 immigration data joined data on i94port 
2 dimension tables 
    1. dimension table - city temperature data
    2. dimension table - the events from the I94 immigration data
    
##fact table:
The fact table will contain information from the I94 immigration data joined with the city temperature data on i94port:

    i94yr  = 4 digit year
    i94mon  = numeric month
    i94cit  = 3 digit code of origin city
    i94port  = 3 character code of destination city
    arrdate = arrival date
    i94mode = 1 digit travel code
    depdate = departure date
    i94visa = reason for immigration
    AverageTemperature  = average temperature of destination city
 
##dimension table：
The temperature dimension table will contain city temperature data. The columns below will be extracted from the temperature data:   

    city temperature data
        i94port = 3 character code of destination city (mapped from immigration data during cleanup step)
        AverageTemperature = Average temperature of the country
        City = city name
        Country = country name
        Latitude = latitude
        Longitude = longitude
        
##The immigration dimension table will contain events from the I94 immigration data. The columns below will be extracted from the immigration data:   

    immigration data
        i94yr = 4 digit year
        i94mon = numeric month
        i94cit = 3 digit code of origin city
        i94port = 3 character code of destination city
        arrdate = arrival date
        i94mode = 1 digit travel code
        depdate = departure date
        i94visa = reason for immigration