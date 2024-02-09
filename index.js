const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

const bigquery = new BigQuery();
const datasetId = 'Weather1';
const tableId = 'Weather Table';

exports.readObservation = (file, context) => {
    const stationId = file.name.split('-')[0]; // Adjust if the format is different
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .pipe(csv())
    .on('data', (data) => transformAndInsertData(data, stationId))
    .on('end', () => {
        console.log('CSV file has been processed');
    });
};

function transformAndInsertData(data, stationId) {
    const transformedData = {
        station: stationId,
        year: data.year,
        month: data.month,
        day: data.day,
        hour: data.hour,
        winddirection: data.winddirection,
        sky: data.sky,
        airtemp: transformNumericValue(data.airtemp),
        dewpoint: transformNumericValue(data.dewpoint),
        pressure: transformNumericValue(data.pressure),
        windspeed: transformNumericValue(data.windspeed),
        precip1hour: transformNumericValue(data.precip1hour),
        precip6hour: transformNumericValue(data.precip6hour)
    };

    // Insert into BigQuery
    bigquery.dataset(datasetId).table(tableId).insert([transformedData])
    .then(() => console.log('Inserted 1 row into BigQuery'))
    .catch(err => console.error('ERROR:', err));
}

function transformNumericValue(value) {
    return value === '-9999' ? null : Number(value) / 10;
}
