from data_gen import Generator
import time


gen = Generator(10000000)
start = time.time()
gen.get_next_min()
print(time.time()-start)
