
import numpy as np
import time
import datetime


class Generator():
    """Generates faux-data for proximity_secure customers"""

    def __init__(self):
        self.minute_steps = 60
        self.num_cust = int(1e7) #10 million
        self.data_type = 'phone'
        self.timestep = datetime.timedelta(0,60) #60 seconds

        self.crnt_timestamp = self.get_time()
        self.crnt_locations = None
        self.crnt_user_data = self.get_user_data()




    def __init__(self, data_type, num_of_customers, minute_steps):
        if type(data_type) is not str:
            raise ValueError('data_type must be a string but user gave type: ' + str(type(data_type)))

        if data_type not in ['phone','transaction']:
            raise ValueError('data_type currently supports "phone" or "transaction" generation but user gave: ' + data_type)

        if type(num_of_customers) is not int:
            raise ValueError('num_of_customers should be an int but user gave: ' + str(type(num_of_customers)))
        
        self.minute_steps = minute_steps
        self.num_cust = num_of_customers
        self.data_type = data_type
        if data_type is 'phone':
            self.timestep = datetime.timedelta(0,60) #60 seconds
        else:
            self.timestep = datetime.timedelta(0,60*60*2) #2 hours

        self.crnt_timestamp = self.get_time()
        self.crnt_locations = None
        self.crnt_user_data = self.get_user_data()




    # generate data for all users for the specified number of minutes
    def gen_user_data_all(self):
        steps = self.minute_steps

        for i in range(steps):
            self.crnt_user_data.extend(self.get_user_data())

        return self.crnt_user_data


    def get_user_data(self):
        if self.crnt_locations is None:
            self.crnt_locations = self.get_coords()
        else:
            self.update_coords()
            self.crnt_timestamp = self.crnt_timestamp + self.timestep

        user_data = []

        for i in range(self.num_cust):
            user_data.append(self.get_user_entry(i,self.crnt_locations[i]))

        return user_data


    # Phone record fields      Format                 Description:
    #   Customer ID     =>     <#>                    1 to self.num_Cust (primary key)
    #   Timestamp       =>     <DDMMYYYY:SSMMHH?>     Time of generation
    #   ?Velocity       =>     <#>                     
    #   Location        =>     <"lat":#, "lng":#>
    #   ?Address        =>     <func(location)>

    def get_user_entry(self, user_ID, location):

        user_entry = {}
        location_str = self.data_type+'_loc'

        user_entry['user_ID'] = user_ID
        user_entry['timestamp'] = self.crnt_timestamp.isoformat(sep=' ')
        user_entry[location_str] = location
        return user_entry

    def get_time(self):
        date = datetime.datetime(2019, 2, 14, 16)
        return date



    # GPS generation
    # Want coordinates to be on landmass
    # Chose a square on the condinetal United States
    #
    #   Upper Left - Lat:47 Long:-122       Upper Right - Lat:47 Long:-92
    #
    #   Lower Left - Lat:37 Long:-122       Lower Right - Lat:37 Long:-92
    #
    def get_coords(self, lat_range=[37,47], lng_range=[-122,-92]):

        locations = []
        samples = self.num_cust

        def get_latitude():
            latitude_array = np.random.uniform(lat_range[0], lat_range[1], samples)
            return latitude_array

        def get_longitude():
            longitude_array = np.random.uniform(lng_range[0], lng_range[1], samples)
            return longitude_array

        lats = get_latitude()
        lngs = get_longitude()

        for i in range(samples):
            d = self.add_gps_to_dic(lats[i],lngs[i])
            locations.append(d)

        return locations

    def add_gps_to_dic(self, lat, lng):
        d = dict()
        d["lat"] = lat
        d["lng"] = lng
        return d


    def update_coords(self):
        #new_locations = [self.resample_coord(dic) for dic in self.crnt_locations]
        self.crnt_locations = [*map(self.resample_coord, self.crnt_locations)]
        
        #new_locations = []
        #for i in self.crnt_locations:
        #    new_locations.append(self.resample_coord(i))

        #self.crnt_locations = new_locations



    #want to call anything outside of .02 in a minute as flagged
    def resample_coord(self, coord_dic, lat_sig=.005, lng_sig=.005):
        new_lat = np.random.normal(coord_dic['lat'], lat_sig)
        new_lng = np.random.normal(coord_dic['lng'], lng_sig)
        dict_entry = self.add_gps_to_dic(new_lat,new_lng)

        return dict_entry









