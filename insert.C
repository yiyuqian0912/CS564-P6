#include "catalog.h"
#include "query.h"
#include <cstring>
#include <cstdlib>

const Status QU_Insert(const string & relation,
                       const int attrCnt,
                       const attrInfo attrList[]) {
    RelDesc relDesc;
    Status status = relCat->getInfo(relation, relDesc); // Get relation info
    if (status != OK) return RELNOTFOUND;

    AttrDesc* relAttrs;
    int relAttrCnt;
    status = attrCat->getRelInfo(relation, relAttrCnt, relAttrs); // Get attributes info
    if (status != OK) return ATTRNOTFOUND;

    if (attrCnt != relAttrCnt) { // Check if the number of attributes matches
        delete[] relAttrs;
        return BADCATPARM;
    }

    int recordLen = 0;
    for (int i = 0; i < relAttrCnt; i++) {
        recordLen += relAttrs[i].attrLen; // Calculate total record length
    }

    char* recordData = new char[recordLen]; // Allocate memory for the record
    memset(recordData, 0, recordLen);


    for (int i = 0; i < relAttrCnt; i++) { // Fill in the record data
        const AttrDesc& schemaAttr = relAttrs[i];
        bool matched = false;

        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(schemaAttr.attrName, attrList[j].attrName) == 0) {
                const char* value = static_cast<const char*>(attrList[j].attrValue);
                char* dest = recordData + schemaAttr.attrOffset;

                switch (schemaAttr.attrType) {
                    case INTEGER: {
                        int intVal = atoi(value);
                        memcpy(dest, &intVal, schemaAttr.attrLen);
                        break;
                    }
                    case FLOAT: {
                        float floatVal = atof(value);
                        memcpy(dest, &floatVal, schemaAttr.attrLen);
                        break;
                    }
                    case STRING: {
                        strncpy(dest, value, schemaAttr.attrLen);
                        break;
                    }
                    default: {
                        delete[] relAttrs;
                        delete[] recordData;
                        return ATTRTYPEMISMATCH;
                    }
                }

                matched = true;
                break;
            }
        }

        if (!matched) {
            // A required attribute was missing from input
            delete[] relAttrs;
            delete[] recordData;
            return BADCATPARM;
        }
    }

    InsertFileScan scan(relation, status);
    if (status != OK) {
        delete[] relAttrs;
        delete[] recordData;
        return status;
    }

    Record rec;
    rec.data = recordData;
    rec.length = recordLen;

    RID rid;
    status = scan.insertRecord(rec, rid);  // Actual insertion

    // Clean up
    delete[] relAttrs;
    delete[] recordData;

    return OK;
}
