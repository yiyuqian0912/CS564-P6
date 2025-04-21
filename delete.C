#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
//MY IMPLEMENTATION
//This function will delete all tuples in relation satisfying the predicate 
//specified by attrName, op, and the constant attrValue. 
//type denotes the type of the attribute. 
//You can locate all the qualifying tuples using a filtered HeapFileScan.

//Gets data about the attribute to filter on.
//attrCat = name of attribute catalog
Status status;

//Starts scanning the relation.
HeapFileScan scan(relation, status);
if (status != OK) {
    return status;
}

//This is for q_7 when the case does not have an attrName. Then it just deletes all tuples.
if (attrName.empty()) {
    RID rid;

    status = scan.startScan(0, 0, STRING, NULL, EQ); 
	//0 offset and 0 length so it has no specific end. String specified but doesn't matter really, 
	//null = no value used for filtering, EQ = equal to. Just a placeholder since there is no value for the filter.
    if (status != OK) return status;

    while (scan.scanNext(rid) == OK) {
        status = scan.deleteRecord();
        if (status != OK) {
			scan.endScan();
        	return status;
		}
    }
    status = scan.endScan();
	return status;
}

// getInfo : Returns the attribute descriptor record for attribute attrName in relation relName. 
//Uses a scan over the underlying heapfile to get all tuples for relation and check 
//each tuple to find whether it corresponds to attrName. (Or maybe do it the other way 
//around !) This has to be done this way because a predicated HeapFileScan does not 
//allow conjuncted predicates. Note that the tuples in attrcat are of type AttrDesc 
//(structure given above).

AttrDesc attrDesc;
status = attrCat->getInfo(relation, attrName, attrDesc);
if (status != OK) {
    return status;
}

//Got help from my partner implementing this portion
void *convertedFilter = nullptr;

    switch (attrDesc.attrType) {
        case INTEGER: {
            int *intVal = new int(atoi(attrValue));
            convertedFilter = intVal;
            break;
        }
        case FLOAT: {
            float *floatVal = new float(atof(attrValue));
            convertedFilter = floatVal;
            break;
        }
        case STRING:
            convertedFilter = (void *)attrValue;
            break;
        default:
            return ATTRTYPEMISMATCH;
    }

//This filters for records that match the attribute and operator.
status = scan.startScan(attrDesc.attrOffset, 
	attrDesc.attrLen, 
	(Datatype)type,
	(char *)convertedFilter,
	op);

if (status != OK) {
    return status;
}

//While there is still a matching record to scanNext it deletes each matching record.
RID rid;
while (scan.scanNext(rid) == OK) {
    status = scan.deleteRecord();
    if (status != OK) {
        scan.endScan();
        return status;
    }
}

//endScan just ends the scan.
status = scan.endScan();
return status;

//MY IMPLEMENTATION^

//return OK;



}


