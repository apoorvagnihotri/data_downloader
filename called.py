import numpy as np

a = np.eye(100)
b = np.random.rand(100)

c = np.matmul(a, b)
# d = np.linalg.inv(c)

print('done')