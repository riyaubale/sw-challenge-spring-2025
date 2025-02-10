import pandas as pd
import os
from datetime import datetime, time as datetime_time

class OHLCV:
    
    # Choose directory of csv files
    def __init__(self, directory):
        self.directory = directory
    
    # Loads all files
    def load_all_files(self):
        try:
            # all ctg_tick files
            files = sorted([f for f in os.listdir(self.directory) if f.startswith('ctg_tick_')])
            
            # if no files exist
            if not files:
                print("No files found.")
                return None
            
            data = []
            
            # Chunking to read files in chunks
            for i in range(0, len(files), 10): # counts files by 10
                chunk = []
                
                for file in files[i:i + 10]:
                    try:
                        df = pd.read_csv(os.path.join(self.directory, file)) # read file & get file from directory
                        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S.%f') # convert to proper date & time
                        chunk.append(df) # add data
                        
                    except Exception as e:
                        print(f"ERROR: Loading {file}: {e}") # prints exception if file didn't load correctly
                
                if chunk:
                    data.append(pd.concat(chunk, ignore_index=True)) # combine all chunks, prevents duplicates
                    del chunk # delete the temporary data
            
            if data:
                # if data isn't empty, return
                return pd.concat(data, ignore_index=True)
            else:
                return None
            
        except Exception as e:
            print(f"ERROR: Can't access directory: {e}")
            return None

    # Cleans the data
    def clean_data(self, data):
        # makes sure price & size is above 0, timestamp is unique and not duplicated, and all values are valid (not missing)
        return data[(data['Price'] > 0) & (data['Size'] > 0) & ~data['Timestamp'].duplicated(keep='first')].dropna()

    # Checks if the user input for date and time is valid
    def valid_date(self, text):
        while True:
            try:
                date = input(text) # user input for date
                time = input("Enter time (hh:mm:ss): ") # user input for time
                
                # check format, date & time should be able to be split into 3 components
                try:
                    if len(date.split('/')) != 3 or len(time.split(':')) != 3:
                        raise ValueError("Please use mm/dd/yyyy and hh:mm:ss")
                except ValueError as e:
                    print(f"Error: {str(e)}")
                    continue
                
                hours, minutes, seconds = map(int, time.split(':')) # splits time into h, m, s
                input_time = datetime_time(hours, minutes, seconds) # creates time object
                
                # check if time interval is within market hours
                if not (datetime_time(9, 30) <= input_time <= datetime_time(16, 0)):
                    print("Time must be within market hours (9:30 AM and 4:00 PM).")
                    continue
                
                # check if the date is a valid date for given data 
                try:
                    month, day, year = map(int, date.split('/')) # splits date into m, d, y
                    if not (1 <= month <= 12 and 1 <= day <= 31 and year == 2024):
                        raise ValueError("Invalid date.")
                except ValueError as e:
                    print(f"Error: {str(e)}")
                    continue
   
                # checks if the number of days is valid for a given month
                days_in_month = {
                    1: 31,    # January
                    2: 29 if (year % 4 == 0 and     # divisible by 4
                            (year % 100 != 0 or     # not divisible by 10
                            year % 400 == 0))       # divisible by 400
                    else 28,  # February
                    3: 31,    # March
                    4: 30,    # April
                    5: 31,    # May
                    6: 30,    # June
                    7: 31,    # July
                    8: 31,    # August
                    9: 30,    # September
                    10: 31,   # October
                    11: 30,   # November
                    12: 31    # December
                }
                
                # if day is more than given day. already check for less than given day 
                try:
                    if day > days_in_month[month]:
                        raise ValueError(f"Invalid day. {month} has {days_in_month[month]} days.")
                except ValueError as e:
                    print(f"ERROR: {str(e)}")
                
                return datetime.strptime(f"{date} {time}.000000", '%m/%d/%Y %H:%M:%S.%f') # returns date in timestamp format
                
            except ValueError as e:
                print("Invalid format. Please use mm/dd/yyyy and hh:mm:ss.")

    # Parses the time interval input 
    def time_interval(self, text):
        units = {'d': 86400, 'h': 3600, 'm': 60, 's': 1} # days, hours, months, seconds, in seconds
        
        while True:
            try:
                time = input("Please enter a time interval: ").lower() # convert to lowercase
                total = 0 # total seconds
                current = '' # temporary string to concatenate digits to form full numbesr
                
                for char in time:
                    if char.isdigit():
                        current += char # concatenates digit
                    elif char in units:
                        if not current:
                            raise ValueError("There is no number before the unit.")
                        total += int(current) * units[char] # add time to total
                        current = '' # reset temporary string
                    else:
                        raise ValueError("Invalid character.")
                
                if current:
                    raise ValueError("Invalid. Please include an unit.")
                    
                return total
                
            except ValueError as e:
                print(f"ERROR: {e}")

    # Creats the OHLCV bars and exports it to csv
    def ohlcv_bars(self, data, interval_seconds, start, end):
        # filters data for timestamps between start & end & sorts by timestamp
        interval_data = data[(data['Timestamp'] >= start) & (data['Timestamp'] <= end)].sort_values('Timestamp')
        
        # if no data
        if interval_data.empty:
            print("No data in interaval")
            return None
        
        # create data frame for ohlcv bars
        # Grouper - groups time series data into intervals, and then does operation on it after
        ohlcv = pd.DataFrame()
        ohlcv['Open Price'] = interval_data.groupby(pd.Grouper(key='Timestamp', freq=f'{interval_seconds}s'))['Price'].first()
        ohlcv['High Price'] = interval_data.groupby(pd.Grouper(key='Timestamp', freq=f'{interval_seconds}s'))['Price'].max()
        ohlcv['Low Price'] = interval_data.groupby(pd.Grouper(key='Timestamp', freq=f'{interval_seconds}s'))['Price'].min()
        ohlcv['Close Price'] = interval_data.groupby(pd.Grouper(key='Timestamp', freq=f'{interval_seconds}s'))['Price'].last()
        ohlcv['Volume'] = interval_data.groupby(pd.Grouper(key='Timestamp', freq=f'{interval_seconds}s'))['Size'].sum()
        
        ohlcv = ohlcv.dropna() # remove invalid bars
        ohlcv.to_csv('ohlcv_bars.csv') # move data to csv file



if __name__ == "__main__":
    loader = OHLCV("data")
    data = loader.load_all_files() # load data
    
    if data is not None:
        data = loader.clean_data(data) # clean data
        
        # user inputs
        print("\nWe will need two time ranges to create the OHLCV bars.")
        date1 = loader.valid_date("Enter first date (mm/dd/yyyy): ")
        date2 = loader.valid_date("Enter second date (mm/dd/yyyy): ")
        time_int = loader.time_interval("Enter time (hh:mm:ss): ")
        
        # creating the ohlcv bars
        bars = loader.ohlcv_bars(data, time_int, min(date1, date2), max(date1, date2))