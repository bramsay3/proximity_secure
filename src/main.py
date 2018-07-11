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
<<<<<<< HEAD
    print('prod phone time: ',time.time()-start2)
    start3 = time.time()
    trans_prod.start_producing(trans)
    print('prod trans time: ',time.time()-start3)
    end = time.time()  
    duration = end - start1
    print("Total time:",duration, "seconds")
=======
    end = time.time()  
    duration = end - start
    wait = 60 - duration
    prod_start = time.time()

    trans_len = len(trans)
    for i in range(math.floor(wait/2))

    trans_prod.start_producing(trans)
    if duration > 60:
        raise Exception("Data generation exceeded 1 minute")
    print("Data generation time: ",duration, "seconds")
    print("Waiting ",wait," seconds till next generation")
>>>>>>> 58ab06fa7f7f28884227a1205132b3bcf355f0cb
    phone_prod.prod.flush()
    trans_prod.prod.flush()
