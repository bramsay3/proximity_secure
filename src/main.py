from data_gen import Generator
from producers import Production
import time
import math

gen = Generator(1000000)

phone_prod = Production('phone_loc')
trans_prod = Production('trans_loc')

print('purging')
[phone,trans] = gen.get_next_min()
phone_prod.start_producing(phone)
time.sleep(70)
print('1st production')
trans_prod.start_producing(trans)
time.sleep(1)           
phone_prod.prod.flush()
trans_prod.prod.flush()


while True:
    print('generating')
    start1 = time.time()
    [phone,trans] = gen.get_next_min()
    print('gen time: ',time.time()-start1)
    start2 = time.time()
    phone_prod.start_producing(phone)
    print('prod phone time: ',time.time()-start2)
    start3 = time.time()
    trans_prod.start_producing(trans)
    print('prod trans time: ',time.time()-start3)
    end = time.time()  
    duration = end - start1
    print("Total time:",duration, "seconds")

    phone_prod.prod.flush()
    trans_prod.prod.flush()
