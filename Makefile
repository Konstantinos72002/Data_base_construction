hp:
	@echo " Compile hp_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/hp_main.c ./src/record.c ./src/hp_file.c -lbf -o ./build/hp_main -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c ./src/record.c -lbf -o ./build/bf_main -O2;

ht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./src/ht_table.c ./examples/ht_main.c ./src/record.c -lbf -o ./build/ht_main -O2

sht:
	@echo " Compile sht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/record.c ./src/sht_table.c ./src/ht_table.c -lbf -o ./build/sht_main -O2
clean_data:
	rm data.db
clean_index:
	rm index.db
clean: clean_data clean_index