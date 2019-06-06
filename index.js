const Influx = require("influx");
var mqtt = require('mqtt');

//var client  = mqtt.connect('tcp://test.mosquitto.org');
var client = mqtt.connect('tcp://broker.hivemq.com');

let Host = 'localhost';
let Port = 8086;

client.on('connect', function () {                              //MQTT
    client.subscribe('kitt/+/telemetry', function (err) {        //subscribe to topic kitt/_targa_/telemetria
        if (err) {
            console.log(err);
        }
        if (!err) {
            console.log('sottoscritto al topic con successo');
        }
    })
})

client.on('message', function (topic, message) {                //MQTT
    //message is Buffer

    //console.log('messaggio ricevuto stringato: ' + message.toString());

    upload(topic, message);

    //console.log(topic.toString());

})

function upload(topic, message) {

    var msg = JSON.parse(message);

    var arrTop = topic.split("/");
    let targa = arrTop[1];

    console.log('Targa: ' + targa);
    /*console.log("temperature "+msg.temperature);
    console.log("speed "+msg.speed);
    console.log("latitude "+msg.latitude);
    console.log("longitude "+msg.longitude);
    console.log("direction "+msg.direction);
    console.log("#####################################");
    */
    
    let influx;
    //influx.createDatabase(targa);
    influx = new Influx.InfluxDB({
        host: Host,
        database: targa,
        port: Port,
        schema: [{
            measurement: targa,
            tags: "host1",
            fields: {
                temperature: Influx.FieldType.FLOAT,
                speed: Influx.FieldType.FLOAT,
                latitude: Influx.FieldType.FLOAT,
                longitude: Influx.FieldType.FLOAT,
                direction: Influx.FieldType.FLOAT
            },

        }]
    });

    influx.writePoints([                            //CARICAMENTO SU INFLUX
        {
            measurement: targa,
            tags: { host: 'host1' },
            fields: {
                temperature: msg.temperature,
                speed: msg.speed,
                latitude: msg.position.latitude,
                longitude: msg.position.longitude,
                direction: msg.direction
            },
            timestamp: msg.timestamp
        }
    ])
    .catch(error => {
            console.log(error);
            res.sendStatus(500);
    });
    
    //console.log("logging messagges")

}




