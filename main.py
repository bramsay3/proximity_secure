from data_gen import Generator
from producers import Production


gen = Generator(100,60)
data = gen.gen_user_data_all()

prod = Production('phone_loc')

prod.start_producing(data)