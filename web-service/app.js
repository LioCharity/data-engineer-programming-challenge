const http = require('http');
var payloadCheck = require('payload-validator');

const hostname = '127.0.0.1';
const port = 3000;

const server = http.createServer((req, res) => {


	let body = [];
	req.on('data', (chunk) => {
		body.push(chunk);
	}).on('end', () => {
		body = Buffer.concat(body).toString();
		// at this point, `body` has the entire request body stored in it as a string
		console.log("Input Body");
		console.log(body);
		var incomingApiPayload = {"top_N_airport": 0}
		// call the validator
		var result = payloadCheck.validator(
			JSON.parse(body),
			incomingApiPayload,
			["top_N_airport"], // mandatory elements
			false

		);

		// In case of incorrect body, return the error to the user
		if(result["success"] == false){
			res.statusCode = 500;
			res.setHeader('Content-Type', 'appliction/json');
			res.end(JSON.stringify(result));
		}else{
			const spawn = require("child_process").spawn;
			const pythonProcess = spawn('python',["fetch_data.py", JSON.parse(body)["top_N_airport"]]);

			pythonProcess.stdout.on('data', (data) => {
				console.log(`${data}`);
				res.statusCode = 200;
				res.setHeader('Content-Type', 'appliction/json');
				res.end(JSON.stringify(`${data}`));
			});
		}
	});
	
});

server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});
