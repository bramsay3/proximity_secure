from data_gen import Generator
from producers import Production
import time

gen = Generator(100)

phone_prod = Production('phone_loc')
trans_prod = Production('trans_loc')

print('producting')
for i in range(10):
    [phone,trans] = gen.get_next_min()
    print('phone',i)
    time.sleep(.01)
    phone_prod.start_producing(phone)
    print('transaction',i)
    time.sleep(.01)
    trans_prod.start_producing(trans)    
print('done')
