const AWS = require('aws-sdk');
const S3 = new AWS.S3();


// Initiate AWS Lamba Function 
exports.handler = async (event, context) => {
  try {
    const dateToProcess = event.date; // Date to process, e.g., '2023-10-05'
    const sensorName = event.sensorName; // Sensor name, e.g., 'vibesensor-58bf25a871c0-record'

    // Get all raw data files for the specified date and sensor
    const s3Params = {
      Bucket: 'matrix-hub-iot',
      Prefix: `${sensorName}/${dateToProcess}`,
    };
    const dataFiles = await S3.listObjectsV2(s3Params).promise();

    // Read and merge all data files
    const mergedData = [];
    for (const file of dataFiles.Contents) {
      const fileContent = await S3.getObject({ Bucket: 'matrix-hub-iot', Key: file.Key }).promise();
      mergedData.push(JSON.parse(fileContent.Body.toString()));
    }

    // Save the merged data as a single file in the processed bucket
    const dailyFilePath = `sensor-data/${sensorName}/${dateToProcess}.json`;
    await S3.putObject({ Bucket: 'matrix-hub-iot-testing', Key: dailyFilePath, Body: JSON.stringify(mergedData) }).promise();

    // Create the sensor mapping file
    const sensorMappingFilePath = `sensor-mapping/${sensorName.split('-')[0]}.json`;
    const sensorMappingData = {
      date: dateToProcess,
      topic: sensorName,
      serial: sensorName.split('-')[1],
    };
    await S3.putObject({ Bucket: 'matrix-hub-iot-testing', Key: sensorMappingFilePath, Body: JSON.stringify(sensorMappingData) }).promise();

    return { statusCode: 200, body: 'Data processing completed.' };
  } catch (error) {
    return { statusCode: 500, body: `Error: ${error.message}` };
  }
};
