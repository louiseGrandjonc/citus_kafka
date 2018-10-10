from ctypes import *


adder = CDLL('./hasher.so')

# def hash_bi(value):
#     return hasher.hash_bigint(value)


# print(hash_bi(1))
# print(hash_bi(2))
# print(hash_bi(3))


# print(hash_bigint(1))
# print(hash_bigint(2))
# print(hash_bigint(3))

res_int = adder.add_int(4,5)
print("Sum of 4 and 5 = " + str(res_int))

#Find sum of floats
a = c_float(5.5)
b = c_float(4.1)

add_float = adder.add_float
add_float.restype = c_float
print("Sum of 5.5 and 4.1 = ", str(add_float(a, b)))
