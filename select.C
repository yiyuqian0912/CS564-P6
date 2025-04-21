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

	// Variable declarations
	Status status;
	AttrDesc *attrDesc = NULL;
	AttrDesc projAttrDesc[projCnt];
	
	// If there's a selection condition, get the attribute info
	if (attr != NULL) {
		attrDesc = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
		if (status != OK) return status;
	}
	
	// Get attribute info for the projection list
	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, 
								projNames[i].attrName, 
								projAttrDesc[i]);
		if (status != OK) return status;
	}
	
	// Calculate the output record length
	int reclen = 0;
	for (int i = 0; i < projCnt; i++) {
		reclen += projAttrDesc[i].attrLen;
	}
	
	// Call ScanSelect to do the actual work
	status = ScanSelect(result, projCnt, projAttrDesc, attrDesc, op, attrValue, reclen);
	
	// Clean up and return
	if (attrDesc != NULL) delete attrDesc;
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

    int resultTuples = 0;
    
    // Setup a scan on the relation
    HeapFileScan *scan = NULL;
    
    // If no attribute descriptor is provided, do an unconditional scan
    if (attrDesc == NULL) {
        scan = new HeapFileScan(projNames[0].relName, status);
        if (status != OK) return status;
        
        // Start an unconditional scan
        status = scan->startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) {
            delete scan;
            return status;
        }
    } else {
        // Set up a filtered scan
        scan = new HeapFileScan(attrDesc->relName, status);
        if (status != OK) return status;
        
        // Convert filter string to the appropriate data type
        void *convertedFilter = NULL;
        
        // Allocate memory for the converted filter value
        switch (attrDesc->attrType) {
            case INTEGER: {
                int intVal = atoi(filter);
                convertedFilter = new int(intVal);
                break;
            }
            case FLOAT: {
                float floatVal = atof(filter);
                convertedFilter = new float(floatVal);
                break;
            }
            case STRING:
                convertedFilter = (void *)filter;
                break;
            default:
                delete scan;
                return ATTRTYPEMISMATCH;
        }
        
        // Start a filtered scan
        status = scan->startScan(attrDesc->attrOffset,
                               attrDesc->attrLen,
                               (Datatype)attrDesc->attrType,
                               (char *)convertedFilter,
                               op);
        
        // Free the converted filter memory if needed
        if (attrDesc->attrType == INTEGER)
            delete (int *)convertedFilter;
        else if (attrDesc->attrType == FLOAT)
            delete (float *)convertedFilter;
        
        if (status != OK) {
            delete scan;
            return status;
        }
    }
    
    // Create an insert file scan for the result relation
    InsertFileScan resultRel(result, status);
    if (status != OK) {
        delete scan;
        return status;
    }
    
    // Allocate memory for output data
    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *)outputData;
    outputRec.length = reclen;
    
    // Scan the relation
    RID rid;
    Record rec;
    
    while (scan->scanNext(rid) == OK) {
        // Get the next record
        status = scan->getRecord(rec);
        ASSERT(status == OK);
        
        // Perform projection by copying only the requested attributes
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            // Copy the attribute value from the source record to the output record
            memcpy(outputData + outputOffset,
                  (char *)rec.data + projNames[i].attrOffset,
                  projNames[i].attrLen);
            
            // Update the offset for the next attribute
            outputOffset += projNames[i].attrLen;
        }
        
        // Insert the projected record into the result relation
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        if (status != OK) {
            delete scan;
            return status;
        }
        
        resultTuples++;
    }
    
    cout << "Selection returned " << resultTuples << " tuples." << endl;
    
    // Clean up
    delete scan;
    return OK;
}
