from data_gen import Generator
from producers import Production


gen = Generator('transaction',5,5)
data = gen.gen_user_data_all()

prod = Production('trans_loc')

prod.start_producing(data,True)
