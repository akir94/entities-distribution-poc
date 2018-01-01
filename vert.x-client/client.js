const EventBus = require("vertx3-eventbus-client");
const eb = new EventBus('http://192.168.0.53:7001/eventbus');

eb.onopen = function() {
  console.log('Opened Connection Successfuly!');
  
    // set a handler to receive a message
    // eb.registerHandler('some-address', function(error, message) {
    //   console.log('received a message: ' + JSON.stringify(message));
    // });
  
    // send a message
    const dataToSend = {
      name: 'Yoav',
      maxLongitude: 40,
      minLongitude: 30,
      maxLatitude: 40,
      minLatitude: 30
    };

	// Statistics
	let count = 0;
	let sum = 0;

    const a = eb.send('setClientState', dataToSend);

	eb.registerHandler(dataToSend.name, function(error, message) {
		if (message.body.action != 'delete') {
			let myDate = new Date();
			let receivedDate = new Date(message.body.distributionTime);
			let lastUpdateTime = new Date(message.body.lastUpdateTime);
			let diffReceivedTime = myDate - receivedDate;
	
			count++;
			sum += diffReceivedTime;

			console.log(diffReceivedTime);
			console.log(myDate - lastUpdateTime);
			console.log('Average: ' + (sum / count));
			console.log('============================');
		}
    		//console.log('received a message: ' + JSON.stringify(message));
 	 });

  };
