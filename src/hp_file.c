#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define HP_ERROR -1

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

#define CALL_BF_POINTER(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return NULL;        \
  }                         \
}

// Δημιουργια ενος κενου Αρχειου με ονομα fileName
int HP_CreateFile(char *fileName){

  CALL_BF(BF_CreateFile(fileName));
  int fptr;
  
  CALL_BF(BF_OpenFile(fileName,&fptr));
  BF_Block* block;
  BF_Block_Init(&block);
  
  // Δεσμευουμε μνημη για το μηδενικο μπλοκ του αρχειου
  CALL_BF(BF_AllocateBlock(fptr,block));
  
  // Αρχικοποιουμε τα δεδομενα για το μηδενικο μπλοκ του αρχειου
  HP_info info;
  info.fptr = fptr;
  info.num_of_records = 0;
  info.blk_counter = 1 ; // (ξεκιναει η αριθμηση απο το 1 για το μηδενικο μπλοκ)

  // Ποσε εγγραφες χωραει το καθε μπλοκ
  info.num_of_records_per_block = (BF_BLOCK_SIZE - sizeof(HP_block_info))/sizeof(Record);
  info.num_of_records = 0;

  // Περναμε τα δεδομενα στο μηδενικο μπλοκ
  char* data;
  data = BF_Block_GetData(block); 
  memcpy(data,&info,sizeof(info));

  // Κανουμε Dirty το block επειδη πειραξαμε τα δεδομενα
  BF_Block_SetDirty(block);
  // Unpin το μπλοκ για να το κατεβασουμε απο την ενδιαμεση μνημη
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(fptr));
  
  return 0;
}

// Ανοιγμα του αρχειου fileName και επιστροφη των πληροφοριων του μηδενικου μπλοκ
HP_info* HP_OpenFile(char *fileName){

  int fptr;
  // Δεσμευουμε μνημη στην σωρο για την πληροφορια
  HP_info *return_info = malloc(sizeof(HP_info));
  BF_Block *block_0;
  BF_Block_Init(&block_0);

  CALL_BF_POINTER(BF_OpenFile(fileName,&fptr));
  // Αντλουμε τα δεδομενα του μηδενικου μπλοκ 
  CALL_BF_POINTER(BF_GetBlock(fptr,0,block_0));
  memcpy(return_info,BF_Block_GetData(block_0),sizeof(*return_info));

  // ανανεωνουμε τον filedesc
  return_info->fptr = fptr;

  // Κατεβαζουμε το μηδενικο μπλοκ απο την ενδιαμεση μνημη
  CALL_BF_POINTER(BF_UnpinBlock(block_0));
  BF_Block_Destroy(&block_0);
  return return_info;
}

// Κλεινουμε το αρχειο και αποδεσμευουμε την μνημη που εχει χρησιμοποιησει
int HP_CloseFile(HP_info* hp_info ){
  int fptr = hp_info->fptr;
  free(hp_info);    
  CALL_BF(BF_CloseFile(fptr));
  return 0;
}

// Εισαγουμε μια εγγραφη
int HP_InsertEntry(HP_info* hp_info, Record record){
  
  BF_Block *hp_block;
  BF_Block_Init(&hp_block);

  // πρωτο byte που θα αποθηκευτει το HP_Block_info
  int offset_for_info = BF_BLOCK_SIZE - sizeof(HP_block_info);   

  // Αν εχει γεμισει (η ειναι η πρωτη μας εγγραφη) το τελευταιο block με εγγραφες τοτε κανε allocate καινουργιο block
  if(hp_info->num_of_records % hp_info->num_of_records_per_block == 0) {
  
    CALL_BF(BF_AllocateBlock(hp_info->fptr, hp_block)); 

    // Δημιουργουμε την πληροφορια που θα αποθηκευτει στο τελος του μπλοκ
    HP_block_info block_info;
    block_info.num_of_records = 0;
    block_info.slot = 0;

    // Την αποθηκευουμε στο τελος του μπλοκ
    char* data;
    data = BF_Block_GetData(hp_block);
    memcpy(data+offset_for_info,&block_info,sizeof(block_info));
  }
    else { // Αμα υπαρχει διαθεσιμη θεση για να γραφτει στο μπλοκ τοτε επιστρεφουμε το τελευταιο μπλοκ που αποθηκευσαμε
    CALL_BF(BF_GetBlock(hp_info->fptr,hp_info->blk_counter,hp_block));
  }
  
  // Αντιγραφουμε το τελος του block (hp_block_info) στο block_info
  HP_block_info block_info;
  memcpy(&block_info, BF_Block_GetData(hp_block) + offset_for_info ,sizeof(block_info));
  
  // data δεικτης στο block αποθηκευσης
  char* data;
  data = BF_Block_GetData(hp_block);
  
  // Προσθεσε την εγγραφη στο slot
  int offset = block_info.slot * sizeof(Record); 
  memcpy(data+offset, &record, sizeof(Record)); 
  
  // Προσθετουμε μια εγγραφη
  hp_info->num_of_records++;

  // Ανανεωνουμε τα δεδομενα του μπλοκ μετα την προσθηκη της εγγραφης
  block_info.num_of_records++;
  block_info.slot++;
  
  // Αν εχει γεμισει το μπλοκ μας
  if(block_info.num_of_records == hp_info->num_of_records_per_block) {
    hp_info->blk_counter++;
    block_info.slot = 0;
    // τοτε πηγαινε σε καινουργιο block
  }

  // Αποθηκευουμε την καινουργια πληροφορια στο μπλοκ
  memcpy(data+offset_for_info,&block_info,sizeof(block_info));
  
  // Ανανεωνουμε την πληροφορια του μηδενικου μπλοκ
  BF_Block* zero_block;
  BF_Block_Init(&zero_block);
  CALL_BF(BF_GetBlock(hp_info->fptr,0,zero_block));
  char* data_0;
  data_0 = BF_Block_GetData(zero_block); 
  memcpy(data_0,hp_info,sizeof(*hp_info));
  BF_Block_SetDirty(zero_block);
  CALL_BF(BF_UnpinBlock(zero_block));
  BF_Block_SetDirty(hp_block); 
  CALL_BF(BF_UnpinBlock(hp_block));

  
  BF_Block_Destroy(&zero_block);
  BF_Block_Destroy(&hp_block); 

  // Επιστρεφουμε το μπλοκ που εγινε η εισαγωγη
  int return_value;
  CALL_BF(BF_GetBlockCounter(hp_info->fptr,&return_value));
  return return_value;  
}


int HP_GetAllEntries(HP_info* hp_info, int value){

  int block_num = 0;   // τα μπλοκ που διαβασαμε
  int slot = 0; // χωρος που θα αντλησουμε την εγγραφη
  BF_Block *block;  
  BF_Block_Init(&block);
  
  // Για το συνολο των εγγραφων 
  for(int i = 0; i < hp_info->num_of_records; i++) {

    // αλλαγη μπλοκ    
    if(i % hp_info->num_of_records_per_block == 0) {
      block_num++;
      slot = 0;
      BF_GetBlock(hp_info->fptr,block_num,block);
    } else {
      // επομενη εγγραφη
      slot++;
    }

    // περνουμε την εγγραφη
    Record record;
    int offset = slot * sizeof(record);
    memcpy(&record,BF_Block_GetData(block) + offset,sizeof(record));
    
    // αν πληρει τις συνθηκες την εκτυπωνουμε (10λεπτα η φωτοτυπια)
    if(record.id == value) {
      printRecord(record);
    }

    // Αν η επομενη εγγραφη ειναι σε αλλο μπλοκ κατεβαζουμε αυτο απο την ενδιάμεση μνημη
    if((i + 1) % hp_info->num_of_records_per_block == 0) {
      BF_UnpinBlock(block);
    }
  }
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
  return 0;
}

