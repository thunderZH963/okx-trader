f = open("./src/symbol_list.json", "r")
import json
symbol_list = json.load(f)
f.close()
f_spot = open("./src/symbol_spot.txt", "w")
f_swap = open("./src/symbol_swap.txt", "w")
for symbol in symbol_list:
    f_spot.write(symbol_list[symbol]['spot'] + "\n")
    f_swap.write(symbol_list[symbol]['swap'] + "\n")
f_spot.close()
f_swap.close()