#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hp_file.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int main() {
  
  // Φτιάχνει ένα αρχείο με όνομα FILE_NAME, το ανοίγει, κάνει insert RECORDS_NUM
  // και καλεί την getallentries για 10 τυχαία ids
  BF_Init(LRU);
  HP_CreateFile(FILE_NAME);
  HP_info* info = HP_OpenFile(FILE_NAME);
  Record record;
  srand(12569874);
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    HP_InsertEntry(info, record);
  }

  printf("RUN PrintAllEntries\n");
  int id[10];
  for(int i = 0; i < 10 ; i++) {
    id[i] = rand()%RECORDS_NUM;
    printf("Searching for: %d\n",id[i]);
    HP_GetAllEntries(info, id[i]);
  }
  HP_CloseFile(info);
  BF_Close();

}
