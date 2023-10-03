import os

def read_chunk(file_object):
    # generator returns generator object
    def gen(file_object):
        yield file_object.read(1024)
    # iterate over next generator object
    return next(gen(file_object))

with open('C:/Users/Craig/Pictures/Brother-HL2270-Wireless.gif', 'rb') as file_object:
    while True:
        file_chunk = read_chunk(file_object)
        if not file_chunk:
            break
        print(len(file_chunk))



    
