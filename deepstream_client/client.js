const deepstream = require('deepstream.io-client-js');
const client = deepstream('192.168.0.53:6020').login();

const minLongitude = process.argv[3];
const maxLongitude = process.argv[4];
const minLatitude = process.argv[5];
const maxLatitude = process.argv[6];
const listName = 'entities_around/' + minLongitude +
                                '/' + maxLongitude +
                                '/' + minLatitude +
                                '/' + maxLatitude;

let list = client.record.getList(listName);
list.subscribe(onListUpdate);

let subscribedEntities = new Set();


function onListUpdate(entityKeys) {
//    console.log("list updated");
    let entries = list.getEntries();
    updateSubs(entries);
}

function updateSubs(entries) {

    for( let i = 0; i < entries.length; i++) {
        let entityKey = entries[i];
        if( !subscribedEntities.has(entityKey) ) {
            client.record.getRecord("entity/" + entityKey).subscribe(entityChanged, true);
            subscribedEntities.add(entityKey);
//            console.log("subscribed to " + entityKey)
        }
    }

    for( let entityKey in subscribedEntities ) {
        if( entries.indexOf( entityKey ) === -1 ) {
            entries.delete(entityKey);
            let record = client.record.getRecord(entityKey);
            record.unsubscribe(entityChanged);
            record.discard();
//            console.log("unsubscribed from " + entityKey)
        }
    }
}

// Statistics
var count = 0;

function entityChanged(data) {
    if(data.triggerTime == null) {
        count += 1;
    } else {
        let date = new Date();
//        let redisDelta = data.redisDelta;
        let triggerTime = new Date(data.triggerTime);
        let triggerDelta = date - triggerTime;

//        console.log(triggerDelta + "," + redisDelta + "," + count);
        console.log(triggerDelta + "," + count);
        count = 0;
    }
}
