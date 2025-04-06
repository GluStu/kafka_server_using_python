def int_to_var_int(n):
    binary_str = "{0:b}".format(n)
    byte_reps = []
    i = len(binary_str)
    for i in range(len(binary_str) - 7, -1, -7):
        arr = binary_str[i : i + 7]
        if i != 0:
            byte_reps.append("1" + arr)
        else:
            byte_reps.append("0" + arr)
    if i != 0:
        s = binary_str[:i]
        byte_reps.append("0" * (8 - len(s)) + s)
    byte_reps = [int(b, 2) for b in byte_reps]
    return byte_reps
def read_varint_from_file(f):
    continue_varint = True
    total_binary_str = ""
    while continue_varint:
        varint_byte = f.read(1)[0]
        if varint_byte >> 7 == 0:
            continue_varint = False
        binary_str = "{0:b}".format(varint_byte)
        binary_str = ("0" * (8 - len(binary_str))) + binary_str
        total_binary_str = binary_str[1:] + total_binary_str
    return int(total_binary_str, 2)
def read_unsigned_varint_from_file(f):
    continue_varint = True
    total_binary_str = ""
    while continue_varint:
        varint_byte = f.read(1)[0]
        if varint_byte >> 7 == 0:
            continue_varint = False
        binary_str = "{0:b}".format(varint_byte)
        binary_str = ("0" * (8 - len(binary_str))) + binary_str
        total_binary_str = binary_str[1:] + total_binary_str
    return int(total_binary_str, 2)
def read_signed_varint_from_file(f):
    continue_varint = True
    total_binary_str = ""
    while continue_varint:
        varint_byte = f.read(1)[0]
        if varint_byte >> 7 == 0:
            continue_varint = False
        binary_str = "{0:b}".format(varint_byte)
        binary_str = ("0" * (8 - len(binary_str))) + binary_str
        total_binary_str = binary_str[1:] + total_binary_str
    return convert_int_to_signed(int(total_binary_str, 2))
def convert_int_to_signed(n):
    return (n >> 1) ^ (-(n & 1))