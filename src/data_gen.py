
import numpy as np
import time
import datetime


class Generator():
    """Generates faux-data for proximity_secure customers"""

    start_time = datetime.datetime(2019, 2, 14, 16)

    def __init__(self):
        self.N = int(1e6) #10 million
        self.timestep = datetime.timedelta(0,60) #60 seconds
        self.crnt_time = self.start_time
        self.prev_locations = init_coords()  # numpy array 1st row is lats 2nd row is lngs

    def __init__(self, num_of_customers):
        if type(num_of_customers) is not int:
            raise ValueError('num_of_customers should be an int but user gave: ' + str(type(num_of_customers)))
        elif num_of_customers < 1:
            raise ValueError('num_of_customers should be a positive number but user gave: ' + str(type(num_of_customers)))

        
        self.N = num_of_customers
        self.timestep = datetime.timedelta(0,60) #60 seconds
        self.crnt_time = self.start_time
        self.prev_locations = self.init_coords()  # numpy array 1st row is lats 2nd row is lngs

    def init_coords(self, lat_range=[37,47], lng_range=[-122,-92]):

        """GPS generation
        Want coordinates to be on landmass
        Chose a square on the condinetal United States
        
           Upper Left - Lat:47 Long:-122       Upper Right - Lat:47 Long:-92
        
           Lower Left - Lat:37 Long:-122       Lower Right - Lat:37 Long:-92
        """

        locations = []
        samples = self.N

        def get_latitude():
            latitude_array = np.random.uniform(lat_range[0], lat_range[1], samples)
            return latitude_array

        def get_longitude():
            longitude_array = np.random.uniform(lng_range[0], lng_range[1], samples)
            return longitude_array

        lats = get_latitude()
        lngs = get_longitude()

        locations = np.vstack((lats,lngs))

        return locations

    def get_next_min(self):
        """Generate data for all users in a minute interval."""
        self.crnt_time += self.timestep
        phone_locations = self.resample_coord(self.prev_locations)

        spenders = self.select_transactions()
        transaction_locations = self.resample_coord(phone_locations[:,spenders])

        phone_data = self.get_entries('phone', phone_locations)
        trans_data = self.get_entries('trans', transaction_locations, spenders)

        self.prev_locations = phone_locations
        return [phone_data, trans_data]

    def select_transactions(self, transaction_rate=1):
        """Chooses which users are making a transaction in this minute
        transaction rate is in trasactions per user per hour"""

        transaction_num = int(np.ceil(transaction_rate * self.N / 60))
        spenders = np.random.choice(self.N, transaction_num, replace=False)

        return spenders

    def resample_coord(self, location_array, lat_sig=.002, lng_sig=.002):
        """Want to call anything outside of .02 in a minute as flagged"""
        
        next_lat_array = np.random.normal(location_array[0], lat_sig)
        next_lng_array = np.random.normal(location_array[1], lng_sig)
        next_locations = np.vstack((next_lat_array, next_lng_array))

        return next_locations

    def get_entries(self, data_type, location, IDs=None):
        if IDs is None:
            IDs = list(range(self.N))
        entries = []
        for i,pair in enumerate(location.T):
            entries.append(self.make_entry(data_type, IDs[i], pair))

        return entries

    def make_entry(self, data_type, user_ID, location):
        """Fields            Format                 Description:
       Customer ID     =>     <#>                    1 to self.N (primary key)
       Timestamp       =>     <DDMMYYYY:SSMMHH?>     Time of generation
       Location        =>     <"lat":#, "lng":#>     Generated from resample_coord
        """
        user_entry = {}
        topic_name = data_type+'_loc'

        user_entry['user_ID'] = user_ID
        user_entry['timestamp'] = self.crnt_time.isoformat(sep=' ')
        user_entry[topic_name] = self.add_gps_to_dic(location[0],location[1])
        return user_entry

    def add_gps_to_dic(self, lat, lng):
        d = dict()
        d["lat"] = lat
        d["lng"] = lng
        return d









