const deepstream = require('deepstream.io-client-js');
const client = deepstream('192.168.0.53:6020').login();

const EVENT_NAME = 'setClientState';
const MY_NAME = 'Alon3';
const dataToSend = {
	name: MY_NAME,
	maxLongitude: 40,
	minLongitude: 30,
	maxLatitude: 40,
	minLatitude: 30
};

// Statistics
let count = 0;
let sum = 0;

client.event.subscribe(MY_NAME, (data) => {
		let date = new Date();
		let lastUpdateTime = new Date(data.lastUpdateTime);
		let receivedTime = new Date(data.distributionTime);
		let diffReceivedTime = date - receivedTime;


	
		if (data.action != 'delete') {
			count++;
			sum += diffReceivedTime;
			console.log('Entity ID: ' + data.id);
			console.log(diffReceivedTime);
			console.log(date - lastUpdateTime);
			console.log('Average: ' + (sum / count));
		}

		console.log("=================================");
		// date = date.getTime()/1000|0;
		// console.log(data.lastUpdateTime);
		// const delay = date - data.lastUpdateTime;
		// console.log('Received Entity ID: ' + data.id + ' ' + delay);

});

client.event.emit(EVENT_NAME, dataToSend);
console.log('Event emitted!');
