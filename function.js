import { cloudEvent, http } from '@google-cloud/functions-framework';
import { Storage } from '@google-cloud/storage';
import { tmpdir } from 'os';
import { sLog, uid, timer } from 'ak-tools';
import dotenv from 'dotenv';
dotenv.config();

/**
 * @typedef {Object} Params
 * @property {string} [foo] - Description of foo
 * @property {number} [bar] - Description of bar
 */

/** @typedef {'/' | '/foo'} Endpoints  */


const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");

const storage = new Storage();
const bucket = storage.bucket('ak-bucky');
const tmp = NODE_ENV === 'dev' ? './tmp' : tmpdir();


// http entry point
// ? https://cloud.google.com/functions/docs/writing/write-http-functions
http('http-entry', async (req, res) => {
	const runId = uid();
	const reqData = { url: req.url, method: req.method, headers: req.headers, body: req.body, runId };
	let response = {};

	try {
		/** @type {Params} */
		const { body = {} } = req;
		/** @type {Endpoints} */
		const { path } = req;

		const t = timer('job');
		t.start();
		sLog(`START: ${req.path}`, reqData);

		//setup the job
		const [job] = route(path);

		// @ts-ignore
		const result = await job(body);
		t.end()
		sLog(`FINISH: ${req.path} ... ${t.report(false).human}`, result);

		//finished
		res.status(200);
		response = result;


	} catch (e) {
		console.error(`ERROR JOB: ${req.path}`, e);
		res.status(500);
		response = { error: e };
	}
	res.send(JSON.stringify(response));
});


// cloud event entry point
// ? https://cloud.google.com/functions/docs/writing/write-event-driven-functions
cloudEvent('event-entry', async (cloudEvent) => {
	const { data } = cloudEvent;
	const runId = uid();
	const reqData = { data, runId };
	let response = {};
	const t = timer('job');
	t.start();
	sLog(`START`, reqData);

	try {
		const result = await main(data);
		sLog(`FINISH ${t.end()}`, { ...result, runId });
		response = result;

	} catch (e) {
		console.error(`ERROR! ${e.message || "unknown"}`, e);
		response = { error: e };
	}

	return response;

});

async function main(data) {
	return Promise.resolve({ status: "ok", message: "service is alive" });
}



/*
----
ROUTER
----
*/

/**
 * determine routes based on path in request
 * @param  {Endpoints} path
 */
function route(path) {
	switch (path) {
		case "/":
			return [main];
		case "/foo":
			return [() => { }];
		default:
			throw new Error(`Invalid path: ${path}`);
	}
}

