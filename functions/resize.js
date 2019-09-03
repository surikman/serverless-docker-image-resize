const stream = require('stream')
const AWS = require('aws-sdk')
const S3 = new AWS.S3({
  signatureVersion: 'v4'
})
const sharp = require('sharp')
const BUCKET = process.env.BUCKET
const URL = `http://${process.env.BUCKET}.s3-website.${process.env.REGION}.amazonaws.com`

// create the read stream abstraction for downloading data from S3
const readStreamFromS3 = ({ Bucket, Key }) => {
  return S3.getObject({ Bucket, Key }).createReadStream()
}
// create the write stream abstraction for uploading data to S3
const writeStreamToS3 = ({ Bucket, Key }) => {
  const pass = new stream.PassThrough()
  return {
    writeStream: pass,
    uploadFinished: S3.upload({
      Body: pass,
      Bucket,
      ContentType: 'image/png',
      Key
    }).promise()
  }
}
// sharp resize stream
const streamToSharp = ({ width, height }) => {
  width = width === 'AUTO' ? null : width;
  height = height === 'AUTO' ? null : height;
  return sharp()
    .resize(width, height)
    .jpeg()
}

exports.handler = async (event) => {
  //https://github.com/sagidM/s3-resizer/blob/master/index.js
  const key = event.queryStringParameters.key
  const match = key.match(/(\d+|AUTO)x(\d+|AUTO)\/(.*)/)
  const width = match[1] === 'AUTO' ? match[1] : parseInt(match[1], 10);
  const height = match[2] === 'AUTO' ? match[2] : parseInt(match[2], 10);
  console.log("Width: "+width, "Height:"+height);
  const originalKey = match[3]
  const newKey = '' + width + 'x' + height + '/' + originalKey
  const imageLocation = `${URL}/${newKey}`

  try {
    // create the read and write streams from and to S3 and the Sharp resize stream
    const readStream = readStreamFromS3({ Bucket: BUCKET, Key: originalKey })
    const resizeStream = streamToSharp({ width, height })
    const { writeStream, uploadFinished } = writeStreamToS3({ Bucket: BUCKET, Key: newKey })

    // trigger the stream
    readStream
      .pipe(resizeStream)
      .pipe(writeStream)

    // wait for the stream to finish
    const uploadedData = await uploadFinished

    // log data to Dashbird
    console.log('Data: ', {
      ...uploadedData,
      BucketEndpoint: URL,
      ImageURL: imageLocation
    })

    // return a 301 redirect to the newly created resource in S3
    return {
      statusCode: '301',
      headers: { 'location': imageLocation },
      body: ''
    }
  } catch (err) {
    console.error(err)
    return {
      statusCode: '500',
      body: err.message
    }
  }
}
