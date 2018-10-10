from ctypes import *


try:
    hasher = CDLL('./adder.so')
except:
    hasher = CDLL('./src/adder.so')

def hash_bi(value):
    return hasher.hash_bigint(value)
