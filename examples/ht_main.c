#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#define RECORDS_NUM 200 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

  // Φτιάχνει ένα αρχείο με όνομα FILE_NAME, το ανοίγει, κάνει insert RECORDS_NUM
  // και καλεί την getallentries για 10 τυχαία ids
  int main() {
    BF_Init(LRU);

    HT_CreateFile(FILE_NAME,10);
    HT_info* info = HT_OpenFile(FILE_NAME);

    Record record;
    srand(12569874);
    printf("Insert Entries\n");
    for (int id = 0; id < RECORDS_NUM; ++id) {
      record = randomRecord();
    HT_InsertEntry(info, record);
    }

    HT_info* daino = HT_OpenFile(FILE_NAME);

    int id[10];
    for(int i = 0; i < 10 ; i++) {
    id[i] = rand()%RECORDS_NUM;
    printf("Searching for: %d\n",id[i]);
    HT_GetAllEntries(info, &id[i]);
    }

    HT_CloseFile(info);
    HT_HashStatistics(FILE_NAME);
    BF_Close();
}
