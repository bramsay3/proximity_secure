import numpy as np



def sample_coord(lat_range=[37,47], lng_range=[-122,-92]):
    num = int(3)
    gps_information = []

    def get_latitude():
        latitude_array = np.random.uniform(lat_range[0], lat_range[1], num)
        return latitude_array

    def get_longitude():
        longitude_array = np.random.uniform(lng_range[0], lng_range[1], num)
        return longitude_array


    def add_2_dict(dic, latitude, longitude):
        dic["lat"] = latitude
        dic["lng"] = longitude
        return None

    lats = get_latitude()
    lngs = get_longitude()
    print(lats)
    print(lngs)

    for i in range(num):
        d = dict()
        d["lat"] = lats[i]
        d["lng"] = lngs[i]
        gps_information.append(d)

    print(gps_information)



sample_coord()
