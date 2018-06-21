from data_gen import Generator
from producers import Production
import time

gen = Generator('transaction',10,1)
prod = Production('trans_loc')

while True:
    data = gen.get_user_data()
    prod.start_producing(data)
    time.sleep(4)
    
