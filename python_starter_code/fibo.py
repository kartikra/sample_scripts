import time
import matplotlib.pyplot as plt

def fibo(n):
	if (n == 1) or (n == 2):
		return 1
	else:
		return fibo(n-1) + fibo(n-2)

def fiboNew(n, computed = {0: 0, 1: 1}):
	if n not in computed:
		computed[n] = fiboNew(n-1, computed) + fiboNew(n-2, computed)
	return computed[n]

fiboExec={}
for x in range(1, 41):
	start_time = time.time()
	f = fibo(x)
	fiboExec[x] = (time.time() - start_time) * (10**6)

fiboNewExec={}
for x in range(1, 41):
	start_time = time.time()
	f = fiboNew(x)
	fiboNewExec[x] = (time.time() - start_time)*(10**6)

x = fiboExec.keys()
y = fiboExec.values()
z = fiboNewExec.values()
plt.plot(x,y)
plt.plot(x,z)
plt.legend(['fiboExec', 'fiboNewExec'], loc='upper left')
plt.show()

