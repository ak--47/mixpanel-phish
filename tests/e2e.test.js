
import { getFunction } from "@google-cloud/functions-framework/testing";
import { jest } from "@jest/globals";
let func;
jest.setTimeout(30_000);

beforeAll(async () => {
	await import("../function.js");
	func = getFunction("http-entry");
});




test("/", async () => {
	const testCase = {
		path: "/"
	};

	let result;
	let status;
	// a Response stub that captures the sent response
	const res = {
		send: x => {
			result = JSON.parse(x);
			return res;
		},
		status: x => {
			status = x;
			return res;
		}
	};

	// invoke the function
	await func(prepReq(testCase), res);
	expect(status).toBe(200);
	expect(result).toEqual({ status: "ok", message: "service is alive" });
	// expect(result.message).toEqual('pong');
});
//ensure request is well formed
function prepReq(req) {
	return {
		body: req.body || {},
		path: req.path,
		method: req.method || "POST",
		headers: req.headers || {
			"content-type": "application/json"
		}
	};
}



test('do tests work', async()=>{
	expect(1).toBe(1)
})

