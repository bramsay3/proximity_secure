from data_gen import Generator
import time


gen = Generator(100)
start = time.time()
save = gen.get_next_min()
print(save)
print(time.time()-start)
