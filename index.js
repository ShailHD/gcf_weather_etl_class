const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

exports.readObservation = (file, context) => {
    console.log(`Reading file: ${file.name}`);

    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', (error) => {
        console.error('Error reading file:', error);
    })
    .pipe(csv())
    .on('data', (row) => {
        printDict(row);
    })
    .on('end', () => {
        console.log('End of CSV file');
    });
};

// Helper function to print each element of the data to the console separately
function printDict(row) {
    for (let key in row) {
        console.log(`${key}: ${row[key]}`);
    }
}
