import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import path from 'path';
import { createWriteStream, createReadStream } from 'fs';
import readline from 'readline';

let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);



export async function touch(filename, data, append = false) {
    const filePath = path.join(TEMP_DIR, filename);
    const writeStream = createWriteStream(filePath, { flags: append ? 'a' : 'w', highWaterMark: 64 * 1024 * 2 });

    return new Promise((resolve, reject) => {
        writeStream.on('error', (e) => {
            if (NODE_ENV === 'dev') debugger;
            reject(e);
        });

        for (const item of data) {
            if (!writeStream.write(JSON.stringify(item) + '\n')) {
                writeStream.once('drain', () => writeStream.end()); // Remove the return here
            }
        }

        writeStream.end();
        writeStream.on('finish', resolve);
    });
}

export async function load(filename, limit = 10000) {
	const filePath = path.join(TEMP_DIR, filename);
	const readStream = createReadStream(filePath, { highWaterMark: 64 * 1024 * 2 });
	const rl = readline.createInterface({
		input: readStream,
		crlfDelay: Infinity,
	});

	const data = [];
	let lines = 0;
	const isDev = NODE_ENV === 'dev'; // Check NODE_ENV once

	return new Promise((resolve, reject) => {
		readStream.on('error', reject); // Handle file read errors

		rl.on('line', (line) => {
			try {
				data.push(JSON.parse(line));
			} catch (error) {
				return reject(new Error(`Failed to parse JSON: ${error.message}`));
			}

			lines++;
			if (isDev && data.length >= limit) {
				rl.close(); // Stop reading if limit is reached in 'dev' mode
			}
		});

		rl.on('close', () => resolve(data));
		rl.on('error', reject); // Handle any readline errors
	});
}

export async function rm(filename) {
	const filePath = join(TEMP_DIR, filename);
	return new Promise((resolve, reject) => {
		unlink(filePath, (err) => {
			if (err) reject(err);
			resolve();
		});
	});
}
