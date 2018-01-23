const deepstream = require('deepstream.io-client-js');
const client = deepstream('192.168.0.53:6020').login();

const EVENT_NAME = 'setClientState';
const PREFIX = 'Alon';
var MY_NAME = PREFIX + process.argv[2]
const dataToSend = {
	name: MY_NAME,
	minLongitude: process.argv[3],
	maxLongitude: process.argv[4],
	minLatitude: process.argv[5],
	maxLatitude: process.argv[6]
};

// Statistics
let count = 0;

client.event.subscribe(MY_NAME, (data) => {
		if(data.triggerTime == null) {
		    count += 1
		} else {
		    let date = new Date();
        	let redisDelta = data.redisDelta;
		    let triggerTime = new Date(data.triggerTime);
		    let triggerDelta = date - triggerTime

//		    console.log("triggerDelta = " + triggerDelta)
//		    console.log("redisDelta = " + redisDelta)
//		    console.log("count = " + count)
		    console.log(triggerDelta + "," + redisDelta + "," + count)
		    count = 0
		}

		//console.log("=================================");

});

client.event.emit(EVENT_NAME, dataToSend);
//console.log('Event emitted!');
