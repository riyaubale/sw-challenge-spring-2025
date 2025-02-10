For the data cleaning, I found the following errors and fixed it by removing them from the dataset. 
1. There were duplicate timestamps, 33,022 of them. I only kept one of them to get unique timestamps.
2. There were missing values, 17,613 of them. 
3. The price was weird, it was either negative or 0 which didn't make sense. There were 35,050 of them. 
4. The timestamps may not have been sorted correctly, so I sorted them from decreasing to increasing time. 
At the end I removed 51,298 rows, so the final dataset now has 1,710,023 rows. 