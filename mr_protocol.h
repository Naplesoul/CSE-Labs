#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
		mr_tasktype taskType;
		int index;
		string filename;
	};

	friend marshall &operator<<(marshall &m, const AskTaskResponse &d)
	{
		int taskType = d.taskType;
		m << taskType << d.index << d.filename;
		return m;
	}

	friend unmarshall &operator>>(unmarshall &u, AskTaskResponse &d)
	{
		int taskType;
		u >> taskType >> d.index >> d.filename;
		d.taskType = (mr_tasktype)taskType;
		return u;
	}

	struct AskTaskRequest {
		// Lab2: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
	};

};

#endif

