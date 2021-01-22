const grpc = require('grpc');
const PROTO_DIR = '../../src/main/proto'
const proto_loader = require('@grpc/proto-loader');

let packageDefinition = proto_loader.loadSync(
    [PROTO_DIR + '/sustain.proto'],
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });

let protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
let sustainService = protoDescriptor.sustain;
let stub = new sustainService.Sustain('localhost:50051', grpc.credentials.createInsecure());


let requestJson = `{
   "collection": "future_heat",
   "feature": "year",
   "label": "temp"
   "gisJoins": [
        "G1201050",
        "G4804550",
        "G4500890"
   ]
}`;

let linearRegressionRequest = {
    request: requestJson
};

let call = stub.LinearRegressionQuery(linearRegressionRequest);

call.on('data', function (response) {
    console.log(response);
});

call.on('end', function () {
    console.log('Completed');
});

call.on('err', function (err) {
    console.log(err);
})
