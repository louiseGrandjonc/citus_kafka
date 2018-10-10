#include <iostream>
#include <stdint.h>

#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

using namespace std;

int32_t hash_bigint(int64_t val) {

  uint32_t lohalf = (uint32_t) val;
  uint32_t hihalf = (uint32_t) (val >> 32);
  register uint32_t a, b, c;

  a = b = c = 0x9e3779b9 + (uint32_t) sizeof(uint32_t) + 3923095;


  lohalf ^= (val >= 0) ? hihalf : ~hihalf;
  a += lohalf;

  c ^= b; c -= rot(b,14); 
  a ^= c; a -= rot(c,11); 
  b ^= a; b -= rot(a,25); 
  c ^= b; c -= rot(b,16); 
  a ^= c; a -= rot(c, 4); 
  b ^= a; b -= rot(a,14); 
  c ^= b; c -= rot(b,24); 

  return c;
}



int add_int(int, int);
float add_float(float, float);

int add_int(int num1, int num2){
    return num1 + num2;
}

float add_float(float num1, float num2){
    return num1 + num2;
}

