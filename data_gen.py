
import numpy as np
import time
import datetime





class Generator():
    """Generates faux-data for proximity_secure customers"""



    __init__(self):
        self.num_cust = int(1e7) #10 million
        self.timestamp = get_time()
        self.locations = None


    __init__(self, num_of_customers):
        if type(num_of_customers) is not int
            raise ValueError('num_of_customers should be an int but was given ' + str(type(num_of_customers)))
        self.num_cust = num_of_customers
        self.timestamp = get_time()
        self.locations = None

    # generate data for all users for the specified number of minutes
    def gen_user_data_all(self, min = 60):
        


    def get_user_data(self):
        if self.locations is None:
            self.locations = get_coords()
        else:


        user_data = []

        for i in range(self.num_cust):
            user_data.append(get_user_entry(i,self.locations[i]))
        return user_data


    # Phone record fields      Format                 Description:
    #   Customer ID     =>     <#>                    1 to self.num_Cust (primary key)
    #   Timestamp       =>     <DDMMYYYY:SSMMHH?>     Time of generation
    #   ?Velocity       =>     <#>                     
    #   Location        =>     <"lat":#, "lng":#>
    #   ?Address        =>     <func(location)>

    def get_user_entry(self, user_ID, phone_loc):

        user_entry = [{},{},{},{},{}]

        user_entry[user_ID]['user_ID'] = user_ID
        user_entry[user_ID]['timestamp'] = self.timestamp
        user_entry[user_ID]['phone_loc'] = phone_loc
        return user_entry

    def get_time():
        start_date = datetime.datetime(2019, 2, 14, 16)
        date_str = start_date.isoformat(sep=' ')
        return date_str



    # GPS generation
    # Want coordinates to be on landmass
    # Chose a square on the condinetal United States
    #
    #   Upper Left - Lat:47 Long:-122       Upper Right - Lat:47 Long:-92
    #
    #   Lower Left - Lat:37 Long:-122       Lower Right - Lat:37 Long:-92
    #
    def get_coords(self, lat_range=[37,47], lng_range=[-122,-92], samples=self.num_cust):

        locations = []

        def get_latitude():
            latitude_array = np.random.uniform(lat_range[0], lat_range[1], samples)
            return latitude_array

        def get_longitude():
            longitude_array = np.random.uniform(lng_range[0], lng_range[1], samples)
            return longitude_array

        lats = get_latitude()
        lngs = get_longitude()

        for i in range(samples):
            d = dict()
            d["lat"] = lats[i]
            d["lng"] = lngs[i]
            locations.append(d)

        return locations

    def update_coords(self):
        #lambda?



    #want to call anything outside of .02 in a minute as flagged
    def resample_coord(self, coord_dic, lat_sig=.005, lng_sig=.005):
        cov = np.diag([lat_sig,lng_sig]) #symetric

        coord = list(coord_dic.values())

        new_coord = np.random.multivarite_normal(np.asarray(coord), cov)









