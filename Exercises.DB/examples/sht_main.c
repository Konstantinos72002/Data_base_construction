#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"

#define RECORDS_NUM 200 // you can change it if you want
#define FILE_NAME "data.db"
#define INDEX_NAME "index.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int main() {
  srand(time(NULL));
  BF_Init(LRU);
  // Αρχικοποιήσεις
  HT_CreateFile(FILE_NAME,10);
  SHT_CreateSecondaryIndex(INDEX_NAME,10,FILE_NAME);
  HT_info* info = HT_OpenFile(FILE_NAME);
  SHT_info* index_info = SHT_OpenSecondaryIndex(INDEX_NAME);
  // // Κάνουμε εισαγωγή τυχαίων εγγραφών τόσο στο αρχείο κατακερματισμού τις οποίες προσθέτουμε και στο δευτερεύον ευρετήριο
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
      Record record = randomRecord();
      int block_id = HT_InsertEntry(info, record);
      SHT_SecondaryInsertEntry(index_info, record, block_id);
  }
    
    char name[10][20];
    for(int i = 0; i < 10 ; i++) {
      Record record = randomRecord();
      strcpy(name[i], record.name);
      printf("Searching for: %s\n",name[i]);
      printf("RUN PrintAllEntries for name %s\n",name[i]);
      SHT_SecondaryGetAllEntries(info,index_info, name[i]);
    }
    // // Κλείνουμε το αρχείο κατακερματισμού και το δευτερεύον ευρετήριο
    HT_HashStatistics(FILE_NAME);
    SHT_HashStatistics(INDEX_NAME);
    SHT_CloseSecondaryIndex(index_info);
    HT_CloseFile(info);
    BF_Close();
}
