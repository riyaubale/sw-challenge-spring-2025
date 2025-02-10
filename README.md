# How to Create Open-High-Low-Close-Volume (OHLCV) Bars

1. Run the code. It may take a second to load the data in, and then will as you for an input
2. For input, enter a date in the MM/DD/YYYY format. Then it will ask you to enter a time in the HH/MM/SS format. If either aren't in the correct format, it will ask you again repeatedly until you enter it in the correct format. 
NOTE: Date must be valid, and must be in 2024. For the date to be valid, the number of days in the month should be correct. For example, you can't input 02/31/2024, as there are never 31 days in February. Moreover the month must be between 1 and 12 inclusive. The time must be between market hours, that is between 09:30:00 and 16:00:00. Keep in mind this is military time. 
3. It will ask you twice for date and time, so consider the interval you want and the date & time will be the endpoints. 
4. Then it will ask you to input a time interval. The format is a number and then a unit (d = day, h = hour, m = minute, s = second) after. For example, if you wanted the time interval to be 1 hour and 30 minutes and 30 seconds, you would input 1h30m30s with no spaces between. If there is an invalid format, it will let you know and ask you again. 
5. After this, you will see a ohlcv.csv file load with the ohlcv bars for the interval and time you inputted. 
6. The ohlcv.csv file will contain data with labels (left to right): Timestamp, open price, high price, low price, close price, and finally volume. 