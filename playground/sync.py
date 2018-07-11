import sys
sys.path.append('../src')
from data_gen import Generator
import time


gen = Generator(1000000)
start = time.time()
save = gen.get_next_min()
print(time.time()-start)
