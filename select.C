#include "catalog.h"
#include "query.h"

/*A selection is implemented using a filtered HeapFileScan.  
The result of the selection is stored in the result relation called result 
(a  heapfile with this name will be created by the parser before QU_Select() is called).  
The project list is defined by the parameters projCnt and projNames.  
Projection should be done on the fly as each result tuple is being appended to the result table.  
A final note: the search value is always supplied as the character string attrValue.  
You should convert it to the proper type based on the type of attr. 
You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float.  
If attr is NULL, an unconditional scan of the input table should be performed*/

// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;
	AttrDesc *attrDesc = NULL;
	AttrDesc projAttrDesc[projCnt];
	
	//if there's a selection condition
	if (attr != NULL) {
		attrDesc = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
		if (status != OK) {
			return status;
		}
	}
	
	//get attribute info for the projection list
	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, 
								projNames[i].attrName, 
								projAttrDesc[i]);
		if (status != OK) {
			return status;
		}
	}
	
	//calculate record length
	int reclen = 0;
	for (int i = 0; i < projCnt; i++) {
		reclen += projAttrDesc[i].attrLen;
	}
	
	//call ScanSelect
	status = ScanSelect(result, projCnt, projAttrDesc, attrDesc, op, attrValue, reclen);

	if (attrDesc != NULL) {
		delete attrDesc;
	}
	return status;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


	Status status;
    
    string scanRelation = (attrDesc ? attrDesc->relName : projNames[0].relName);

    HeapFileScan* scan = new HeapFileScan(scanRelation, status);
    if (status != OK) {
		delete scan;
		return status;
	}
    
    //no attdesc is provided, do an unconditional scan
    if (attrDesc == NULL) {
        //unconditional scan
        status = scan->startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) {
            delete scan;
            return status;
        }
    } else { //filterred scan
        void* convertedFilter = nullptr;
		int intVal;
		float floatVal;
        
        //filter through values
        switch (attrDesc->attrType) {
            case INTEGER:
                intVal = atoi(filter);
                convertedFilter = &intVal;
                break;
            case FLOAT:
                floatVal = atof(filter);
                convertedFilter = &floatVal;
                break;
            case STRING:
                convertedFilter = (void*)filter;
                break;
            default:
                delete scan;
                return ATTRTYPEMISMATCH;
        }
        
        //start a filtered scan
        status = scan->startScan(attrDesc->attrOffset,
                               attrDesc->attrLen,
                               (Datatype)attrDesc->attrType,
                               (char *)convertedFilter,
                               op);
        
        if (status != OK) {
            delete scan;
            return status;
        }
    }
    
    //create an insert file scan for the result relation
    InsertFileScan resultRel(result, status);
    if (status != OK) {
        delete scan;
        return status;
    }
    
    //output buffer
    char* outputData = new char[reclen];
    Record outputRec;
    outputRec.data = outputData;
    outputRec.length = reclen;
    
    //scan relation
    RID rid;
    Record rec;
    
    while (scan->scanNext(rid) == OK) {
        status = scan->getRecord(rec);
        ASSERT(status == OK);
        
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + outputOffset,
                  (char *)rec.data + projNames[i].attrOffset,
                  projNames[i].attrLen);
            
            outputOffset += projNames[i].attrLen;
        }
        
        //insert the projected record into the result relation
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        if (status != OK) {
			delete[] outputData;
            delete scan;
            return status;
        }
    }
	delete[] outputData;
    delete scan;
    return OK;
}
