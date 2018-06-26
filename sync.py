from data_gen import Generator
import time


gen = Generator('phone',1000000,1)
start = time.time()
gen.get_user_data()
print(time.time()-start)
