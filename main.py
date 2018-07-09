from data_gen import Generator
from producers import Production
import time

gen = Generator(100000)

phone_prod = Production('phone_loc')
trans_prod = Production('trans_loc')

print('purging')
[phone,trans] = gen.get_next_min()
phone_prod.start_producing(phone)
time.sleep(120)
print('1st production')
trans_prod.start_producing(trans)
time.sleep(60)           
phone_prod.prod.flush()
trans_prod.prod.flush()


while True:
    print('generating')
    start = time.time()
    [phone,trans] = gen.get_next_min()
    print('producting')
    phone_prod.start_producing(phone)
    trans_prod.start_producing(trans) 
    end = time.time()  
    duration = end - start
    wait = 60 - duration
    if duration > 60:
        raise Exception("Data generation exceeded 1 minute")
    print("Data generation time: ",duration, "seconds")
    print("Waiting ",wait," seconds till next generation")
    time.sleep(wait)
    phone_prod.prod.flush()
    trans_prod.prod.flush()
