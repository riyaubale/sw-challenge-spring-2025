import pandas as pd
import os
from datetime import datetime, timedelta, time

class OHLCV:
    
    # Choose directory of csv files
    def __init__(self, directory):
        self.directory = directory
    
    # Loads all files
    def load_all_files(self):
        data = [] # where we will store all data, empty at first
        
        files = os.listdir(self.directory) # all files
        tick_files = [f for f in files if f.startswith('ctg_tick_')] # ctg_tick files
        tick_files.sort() # sort by timestamp
        
        # Load each file
        for file in tick_files:
            try:
                full_path = os.path.join(self.directory, file) # get file from directory  
                df = pd.read_csv(full_path) # read file
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S.%f') # convert to proper date & time
                df['Date'] = df['Timestamp'].dt.date # extract date into separate column 
                data.append(df) # add data
            except Exception as e:       
                print(f"ERROR: Loading {file}: {e}") # prints exception if file didn't load correctly
        
        # Combine all files into one df
        if data:
            final_df = pd.concat(data, ignore_index = True)
            return final_df
        
        return None # when no data was loaded


    def clean_data(self, data):
        # initial_length = len(data) # length of uncleaned data

        # How many duplicates exist
        #print(f"Duplicates {len(data[data.duplicated(subset=['Timestamp'], keep=False)])}")

        # How many missing values exist
        #print(f"Missing Values: {len(data[data.isnull().any(axis=1)])}")

        # How many invalid prices & sizes exist
        #print(f"Invalid Prices: {len( data[(data['Price'] <= 0) | (data['Price'].isnull())])}")
        #print(f"Invalid Sizes: {len(data[(data['Size'] <= 0) | (data['Size'].isnull())])}")

        data = data.drop_duplicates(subset=['Timestamp'], keep='first') # removes duplicates
        data = data.dropna() # removes missing values
        data = data[(data['Price'] > 0) & (data['Size'] > 0)] # removes invalid prices & sizes

        #print(f"Removed {initial_length - len(data)}")
        #print(f"Total rows: {len(data)}")
        
        return data
    
    # Checks if the input date is valid, if not, keep asking user
    def valid_date(self, text):
        while True:
            try:
                # Get basic date and time
                date = input(text)
                time = input("Enter time (hh:mm:ss): ")
                
                # First validate just hours and minutes for market hours
                basic_time = datetime.strptime(time.split('.')[0], '%H:%M:%S').time()
                market_open = datetime.strptime('09:30:00', '%H:%M:%S').time()
                market_close = datetime.strptime('16:00:00', '%H:%M:%S').time()
                
                if not (market_open <= basic_time <= market_close):
                    print("Time must be within market hours (9:30 AM and 4:00 PM.)")
                    continue
                
                # If within market hours, then parse the full timestamp
                combined = f"{date} {time}"
                if '.' not in time:  # If no microseconds provided, add zeros
                    combined += '.000000'
                    
                return datetime.strptime(combined, '%m/%d/%Y %H:%M:%S.%f')
                
            except ValueError:
                print("Invalid format. Please use: mm/dd/yyyy for date and hh:mm:ss for time")
          
    # Gets the time interval & parse it     
    def time_interval(self, text):
        # keep trying until user gives input with correct format
        while True:
            time_int = input("Please enter a time interval: ") # user input
            try:
                total_time = 0 # total number of seconds
                current = "" # which character in string we're currently parsing
                
                for char in time_int:
                    # makes string of all digits before the letter
                    if char.isdigit():
                        current += char
                        
                    # if char is one of the following letters, convert the number into seconds
                    elif char in ['d', 'h', 'm', 's']:
                        
                        # if we don't have a number before the letter, empty string
                        if not current:
                            raise ValueError("Time interval isn't valid.")
                        
                        i = int(current) # convert to int
                        if char == 'd':
                            # seconds in a day
                            total_time += i * 60 * 60 * 24
                        elif char == 'h':
                             # seconds in a hour
                            total_time += i * 60 * 60
                        elif char == 'm':
                             # seconds in a min
                            total_time += i * 60
                        elif char == 's':
                            total_time += i
                        
                        current = "" # reset our temporary current string while counting
                    
                    else:
                        raise ValueError("Input isn't valid.")
                
                if current: 
                    # if there are numbers after the digits
                    raise ValueError("Input isn't valid.")
                    
                return total_time
                
            except ValueError as e:
                print(f"Error: {e}")
                
                
    def ohlcv_bars(self, data, time_int, start, end):
        ohlcv_bars = [] 
        
        data = data[(data['Timestamp'] >= start) & (data['Timestamp'] <= end)] # filter data so it fits in interval
        
        # if data isn't in time interval
        if len(data) == 0:
            return None  
        
        current = data['Timestamp'].min() # get first timestamp within interval
        
        while current <= end:
        # Convert current timestamp to datetime
            current_dt = current.to_pydatetime()
            # Add seconds to get end time
            end_dt = current_dt + timedelta(seconds=time_int)
            # Convert back to pandas timestamp
            end = pd.Timestamp(end_dt)
            
            # Get data for this interval
            mask = (data['Timestamp'] >= current) & (data['Timestamp'] < end)
            interval_data = data[mask]
            
            # if there is data within this interval
            if len(interval_data) > 0:
                bar = {
                    'Start Time': current,
                    'End Time': end,
                    'Open Price': interval_data['Price'].iloc[0], # first element   
                    'High Price': interval_data['Price'].max(), # maximum     
                    'Low Price': interval_data['Price'].min(), # minimum       
                    'Close Price': interval_data['Price'].iloc[-1], # last element 
                    'Volume': interval_data['Size'].sum() # sum of all sizes     
                }
                
                ohlcv_bars.append(bar)
            
            current = end # next interval
        
        bars_df = pd.DataFrame(ohlcv_bars) # converts to data frame
        bars_df.to_csv('ohlcv_bars.csv', index=False) # converts to csv file
        print(f"Created {len(bars_df)} bars")
        
        return bars_df

        

if __name__ == "__main__":
    loader = OHLCV("data")
    data = loader.load_all_files() # loads all files from data directory
    
    if data is not None:
        data = loader.clean_data(data)
        
    # get the two input dates
    print("\nWe need two dates to produce the OHLCV bars.")
    date1 = loader.valid_date("Please enter the first date (mm/dd/yyyy): ")
    date2 = loader.valid_date("Please enter the second date (mm/dd/yyyy): ")
    
    # get the time interval
    time_int = loader.time_interval("Please enter a time interval: ")
    
    # get start & end dates
    start = min(date1, date2)
    end = max(date1, date2)
    
    # create the ohlcv bars
    bars = loader.ohlcv_bars(data, time_int, start, end)
    
    
    